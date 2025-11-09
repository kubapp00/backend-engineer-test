import Fastify, { type FastifyRequest, type FastifyReply } from 'fastify';
import { Pool, PoolClient } from 'pg';
import { calculateBlockHash, Block, Input, Output, parseValue, Transaction } from './utils.js';
import 'node:crypto';

const fastify = Fastify({ logger: true });
let dbPool: Pool;

async function createTables(pool: PoolClient) {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS blocks (
            height INTEGER PRIMARY KEY,
            id TEXT UNIQUE NOT NULL,
            transaction_ids TEXT[] NOT NULL 
        );
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS utxos (
            tx_id TEXT NOT NULL,
            output_index INTEGER NOT NULL,
            address TEXT NOT NULL,
            value NUMERIC(20, 8) NOT NULL,
            block_height INTEGER NOT NULL,
            spent_tx_id TEXT DEFAULT NULL,
            PRIMARY KEY (tx_id, output_index)
        );
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS utxo_address_unspent_idx ON utxos (address) WHERE spent_tx_id IS NULL;`);
    await pool.query(`CREATE INDEX IF NOT EXISTS utxo_spent_tx_id_idx ON utxos (spent_tx_id);`);
}

async function getCurrentHeight(client: PoolClient | Pool): Promise<number> {
    const result = await client.query(`SELECT MAX(height) as max_height FROM blocks`);
    const maxHeight = result.rows[0]?.max_height;
    return maxHeight ? Number(maxHeight) : 0;
}

async function processBlock(client: PoolClient, block: Block): Promise<void> {
    const currentHeight = await getCurrentHeight(client);
    if (block.height !== currentHeight + 1) {
        throw new Error(`Invalid block height: Expected ${currentHeight + 1}, got ${block.height}.`);
    }
    const txIds = block.transactions.map((t: Transaction) => t.id);
    const expectedHash = await calculateBlockHash(block.height, txIds);
    if (block.id !== expectedHash) {
        throw new Error(`Invalid block ID: Expected hash mismatch. Computed: ${expectedHash}, Got: ${block.id}.`);
    }
    const inputsToSpend: { txId: string, index: number, spenderId: string }[] = [];
    const newUtxos: Omit<Input & Output & { block_height: number, output_index: number }, 'index'>[] = [];
    for (const tx of block.transactions) {
        let txInputsValue = 0;
        let txOutputsValue = 0;
        if (tx.inputs.length > 0) {
            const inputReferences = tx.inputs.map((i: Input) => `'${i.txId}_${i.index}'`).join(',');
            const utxoQuery = await client.query(`
                SELECT tx_id, output_index, value, spent_tx_id
                FROM utxos 
                WHERE CONCAT(tx_id, '_', output_index) IN (${inputReferences})
            `);
            if (utxoQuery.rows.length !== tx.inputs.length) {
                throw new Error(`Transaction ${tx.id}: Missing one or more referenced UTXOs.`);
            }
            for (const input of tx.inputs) {
                const utxo = utxoQuery.rows.find((r: any) => r.tx_id === input.txId && r.output_index === input.index);
                if (!utxo) { throw new Error(`Transaction ${tx.id}: UTXO ${input.txId}:${input.index} not found in database.`); }
                if (utxo.spent_tx_id !== null) { throw new Error(`Transaction ${tx.id}: UTXO ${input.txId}:${input.index} is already spent by ${utxo.spent_tx_id}.`); }
                const value = parseValue(utxo.value);
                txInputsValue += value;
                inputsToSpend.push({ txId: input.txId, index: input.index, spenderId: tx.id });
            }
        }
        tx.outputs.forEach((output: Output, index: number) => {
            txOutputsValue += parseValue(output.value);
            newUtxos.push({
                txId: tx.id,
                output_index: index,
                address: output.address,
                value: output.value,
                block_height: block.height,
            });
        });
        if (tx.inputs.length > 0 && Number(txInputsValue.toFixed(8)) !== Number(txOutputsValue.toFixed(8))) {
            throw new Error(`Transaction ${tx.id}: Input sum (${txInputsValue.toFixed(8)}) does not equal Output sum (${txOutputsValue.toFixed(8)}).`);
        }
    }
    await client.query(`INSERT INTO blocks (height, id, transaction_ids) VALUES ($1, $2, $3);`, [block.height, block.id, txIds]);
    if (inputsToSpend.length > 0) {
        for (const input of inputsToSpend) {
            await client.query(`
                UPDATE utxos 
                SET spent_tx_id = $3
                WHERE tx_id = $1 AND output_index = $2 AND spent_tx_id IS NULL;
            `, [input.txId, input.index, input.spenderId]);
        }
    }
    if (newUtxos.length > 0) {
        const values: any[] = [];
        const paramPlaceholders = newUtxos.map((u, i) => {
            values.push(u.txId, u.output_index, u.address, u.value, u.block_height, null);
            const start = i * 6;
            return `($${start + 1}, $${start + 2}, $${start + 3}, $${start + 4}, $${start + 5}, $${start + 6})`;
        }).join(',');
        const finalQuery = `
            INSERT INTO utxos (tx_id, output_index, address, value, block_height, spent_tx_id)
            VALUES ${paramPlaceholders};
        `;
        await client.query(finalQuery, values);
    }
}

async function rollbackToHeight(client: PoolClient, targetHeight: number): Promise<number[]> {
    const currentHeight = await getCurrentHeight(client);
    if (targetHeight >= currentHeight) { return []; }
    if (targetHeight < 0) { throw new Error(`Target height must be non-negative.`); }
    const txIdsResult = await client.query(`
        SELECT transaction_ids FROM blocks WHERE height > $1
    `, [targetHeight]);
    const txIdsInRolledBackBlocks = txIdsResult.rows.flatMap((r: any) => r.transaction_ids);
    if (txIdsInRolledBackBlocks.length > 0) {
        await client.query(`
            UPDATE utxos
            SET spent_tx_id = NULL
            WHERE spent_tx_id = ANY($1::text[]);
        `, [txIdsInRolledBackBlocks]);
    }
    await client.query(`
        DELETE FROM utxos 
        WHERE block_height > $1;
    `, [targetHeight]);
    const blocksDeletedResult = await client.query(`
        DELETE FROM blocks 
        WHERE height > $1
        RETURNING height;
    `, [targetHeight]);
    return blocksDeletedResult.rows.map((r: any) => Number(r.height));
}

fastify.post('/blocks', async (request, reply) => {
    const block = request.body as Block;
    if (typeof block.height !== 'number' || !Array.isArray(block.transactions) || !block.id) {
        reply.code(400).send({ error: 'Invalid Block structure: Missing height, id, or transactions array.' });
        return;
    }
    const client = await dbPool.connect();
    try {
        await client.query('BEGIN');
        await processBlock(client, block);
        await client.query('COMMIT');
        reply.code(201).send({ status: 'Block indexed successfully', height: block.height });
    } catch (error) {
        await client.query('ROLLBACK');
        const errorMessage = error instanceof Error ? error.message : 'Unknown indexing error.';
        fastify.log.warn(`Block indexing failed (Height: ${block.height}): ${errorMessage}`);
        reply.code(400).send({ error: errorMessage });
    } finally {
        client.release();
    }
});

fastify.get('/balance/:address', async (request, reply) => {
    const { address } = request.params as { address: string };
    try {
        const result = await dbPool.query(`
            SELECT COALESCE(SUM(value), 0) AS balance
            FROM utxos
            WHERE address = $1 AND spent_tx_id IS NULL;
        `, [address]);
        const balance = parseValue(result.rows[0].balance);
        reply.code(200).send({ address, balance });
    } catch (error) {
        fastify.log.error(error);
        reply.code(500).send({ error: 'Internal server error while fetching balance.' });
    }
});

fastify.post('/rollback', async (request, reply) => {
    const query = request.query as { height: string };
    const targetHeight = Number(query.height);
    if (isNaN(targetHeight) || query.height === undefined) {
        reply.code(400).send({ error: 'Invalid or missing target height in query parameters.' });
        return;
    }
    const client = await dbPool.connect();
    try {
        await client.query('BEGIN');
        const rolledBackHeights = await rollbackToHeight(client, targetHeight);
        await client.query('COMMIT');
        if (rolledBackHeights.length === 0) {
             reply.code(200).send({ status: `Indexer state unchanged. Already at or below height ${targetHeight}.` });
        } else {
             reply.code(200).send({ status: `Successfully rolled back to height ${targetHeight}.`, blocks_removed: rolledBackHeights });
        }
    } catch (error) {
        await client.query('ROLLBACK');
        const errorMessage = error instanceof Error ? error.message : 'Unknown rollback error.';
        fastify.log.warn(`Rollback failed (Target Height: ${targetHeight}): ${errorMessage}`);
        reply.code(400).send({ error: errorMessage });
    } finally {
        client.release();
    }
});

fastify.get('/', async (request, reply) => {
    return { hello: 'world' };
});

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function bootstrap() {
    console.log('Bootstrapping indexer...');
    const databaseUrl = process.env.DATABASE_URL;
    if (!databaseUrl) {
        throw new Error('DATABASE_URL is required');
    }
    dbPool = new Pool({ connectionString: databaseUrl, connectionTimeoutMillis: 5000 });
    let retries = 5;
    while (retries > 0) {
        try {
            const client = await dbPool.connect();
            console.log("Database connection test successful.");
            await createTables(client);
            console.log('Database tables verified/created successfully.');
            const currentHeight = await getCurrentHeight(client);
            console.log(`Current indexed blockchain height: ${currentHeight}`);
            client.release();
            break;
        } catch (err: any) {
            retries--;
            console.error(`Failed to connect to database during bootstrap (Retries left: ${retries}):`, err.message);
            if (retries === 0) {
                console.error("Out of retries. Shutting down.");
                throw err;
            }
            await delay(3000);
        }
    }
}

try {
    await bootstrap();
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
} catch (err) {
    fastify.log.error(err)
    process.exit(1)
};