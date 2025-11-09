import { webcrypto as crypto } from 'node:crypto';

export type Output = { address: string; value: number }
export type Input = { txId: string; index: number }
export type Transaction = { id: string; inputs: Array<Input>; outputs: Array<Output> }
export type Block = { id: string; height: number; transactions: Array<Transaction>; }

export async function calculateBlockHash(height: number, transactionIds: string[]): Promise<string> {
  const data = height.toString() + transactionIds.join('');
  const buffer = new TextEncoder().encode(data);
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  return hashHex;
}

export function parseValue(value: any): number {
  const num = Number(value);
  if (isNaN(num)) { return 0; }
  return num;
}