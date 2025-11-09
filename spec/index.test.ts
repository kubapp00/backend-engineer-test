import { test, expect } from "bun:test";
import { Block, calculateBlockHash } from "../src/utils.js";
import { Pool } from 'pg';

const API_URL = 'http://localhost:3000';
const TEST_DB_URL = process.env.DATABASE_URL || 'postgres://myuser:mypassword@db:5432/mydatabase';

const testPool = new Pool({ connectionString: TEST_DB_URL });

async function postBlock(block: Block): Promise<Response> {
    return fetch(`${API_URL}/blocks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(block)
    });
}

async function getBalance(address: string): Promise<number> {
    const response = await fetch(`${API_URL}/balance/${address}`);
    if (response.status !== 200) {
        const errorData = await response.text();
        console.error(`API error fetching balance: ${response.status}`, errorData);
        throw new Error(`API error fetching balance: ${response.status} - ${errorData}`);
    }
    const data = await response.json();
    return data.balance;
}

async function postRollback(height: number): Promise<Response> {
    return fetch(`${API_URL}/rollback?height=${height}`, {
        method: 'POST',
    });
}

async function clearDatabase() {
    let retries = 5;
    while(retries > 0) {
        try {
            const client = await testPool.connect();
            try {
                await client.query('DELETE FROM utxos;');
                await client.query('DELETE FROM blocks;');
            } finally {
                client.release();
            }
            return; 
        } catch (err) {
            console.log("Failed to connect to test DB for cleaning, retrying...");
            retries--;
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
    throw new Error("Failed to clear database.");
}

async function waitForApi() {
    let retries = 20; 
    console.log("Waiting for API to be ready...");
    while (retries > 0) {
        try {
            const response = await fetch(`${API_URL}/balance/test`, { signal: AbortSignal.timeout(1000) });
            if (response.status > 0) { 
                console.log("API is ready.");
                return; 
            }
        } catch (error) {
        }
        retries--;
        await new Promise(resolve => setTimeout(resolve, 2000));
    }
    throw new Error("API failed to start within the maximum timeout (40 seconds).");
}

const BLOCK_1: Block = { height: 1, id: "d1582b9e2cac15e170c39ef2e85855ffd7e6a820550a8ca16a2f016d366503dc", transactions: [{ id: "tx1", inputs: [], outputs: [{ address: "addr1", value: 10 }] }] };
const BLOCK_2: Block = { height: 2, id: "c4701d0bfd7179e1db6e33e947e6c718bbc4a1ae927300cd1e3bda91a930cba5", transactions: [{ id: "tx2", inputs: [{ txId: "tx1", index: 0 }], outputs: [{ address: "addr2", value: 4 }, { address: "addr3", value: 6 }] }] };
const BLOCK_3: Block = { height: 3, id: "4e5f22a2abacfaf2dcaaeb1652aec4eb65028d0f831fa435e6b1ee931c6799ec", transactions: [{ id: "tx3", inputs: [{ txId: "tx2", index: 1 }], outputs: [{ address: "addr4", value: 2 }, { address: "addr5", value: 2 }, { address: "addr6", value: 2 }] }] };


test('E2E: Indexing, Balance, Rollback, and Validation Scenario', async () => {
    await waitForApi();
    await clearDatabase(); 

    console.log("Test 1: Invalid Height (Expected 400)");
    const invalidHeightBlock: Block = { ...BLOCK_1, height: 2 };
    const resHeightError = await postBlock(invalidHeightBlock);
    expect(resHeightError.status).toBe(400);
    
    console.log("Test 2: Invalid Hash (Expected 400)");
    const invalidHashBlock: Block = { ...BLOCK_1, id: "totally_wrong_hash" };
    const resHashError = await postBlock(invalidHashBlock);
    expect(resHashError.status).toBe(400);

    console.log("Test 3: Indexing Block 1 (Expected 201)");
    const res1 = await postBlock(BLOCK_1);
    if (res1.status !== 201) {
        console.error("Block 1 failed:", await res1.text());
    }
    expect(res1.status).toBe(201);
    
    console.log("Test 3.1: Checking balances after Block 1");
    expect(await getBalance("addr1")).toBe(10);
    expect(await getBalance("addr2")).toBe(0);
    
    console.log("Test 4: Indexing Block 2 (Expected 201)");
    const res2 = await postBlock(BLOCK_2);
     if (res2.status !== 201) {
        console.error("Block 2 failed:", await res2.text());
    }
    expect(res2.status).toBe(201);
    
    console.log("Test 4.1: Checking balances after Block 2");
    expect(await getBalance("addr1")).toBe(0); 
    expect(await getBalance("addr2")).toBe(4); 
    expect(await getBalance("addr3")).toBe(6); 
    
    console.log("Test 5: Indexing Block 3 (Expected 201)");
    const res3 = await postBlock(BLOCK_3);
     if (res3.status !== 201) {
        console.error("Block 3 failed:", await res3.text());
    }
    expect(res3.status).toBe(201);
    
    console.log("Test 5.1: Checking balances after Block 3");
    expect(await getBalance("addr1")).toBe(0);
    expect(await getBalance("addr2")).toBe(4); 
    expect(await getBalance("addr3")).toBe(0); 
    expect(await getBalance("addr4")).toBe(2);
    
    console.log("Test 6: Double Spend (Expected 400)");
    const doubleSpendBlock: Block = {
        height: 4,
        id: await calculateBlockHash(4, ["tx4_double_spend"]),
        transactions: [{
            id: "tx4_double_spend",
            inputs: [{ txId: "tx2", index: 1 }], 
            outputs: [{ address: "hacker", value: 6 }] 
        }]
    };
    const resDoubleSpendError = await postBlock(doubleSpendBlock);
    expect(resDoubleSpendError.status).toBe(400);
    
    console.log("Test 7: Rollback to height 2 (Expected 200)");
    const resRollback = await postRollback(2);
    expect(resRollback.status).toBe(200);
    const dataRollback = await resRollback.json();
    expect(dataRollback.blocks_removed).toEqual([3]);

    console.log("Test 7.1: Checking balances after Rollback");
    expect(await getBalance("addr1")).toBe(0);
    expect(await getBalance("addr2")).toBe(4); 
    expect(await getBalance("addr3")).toBe(6); 
    expect(await getBalance("addr4")).toBe(0); 
    
    console.log("Test 8: Re-Indexing Block 3 (Expected 201)");
    const finalBlock3: Block = {
        ...BLOCK_3,
        height: 3, 
        id: BLOCK_3.id
    };
    const resFinal = await postBlock(finalBlock3);
     if (resFinal.status !== 201) {
        console.error("Block 3 (Final) failed:", await resFinal.text());
    }
    expect(resFinal.status).toBe(201);

    console.log("Test 8.1: Final balance check");
    expect(await getBalance("addr3")).toBe(0); 
    expect(await getBalance("addr4")).toBe(2);
}, 60000);