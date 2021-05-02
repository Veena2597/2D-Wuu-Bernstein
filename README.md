There are 3 client servers (each client server is coded independently for simplicity). Each client maintains 3 local data structures:
1. a blockchain
2. a balance table containing the balance of all clients in the systems based on the information it has in its local blockchain
3. a 2 dimensional Wuu and Bernstein table

When a client executes a transfer transaction, it will record this transaction by updating its local copy of the blockchain. A client who receives money will not know about it until it receives a message with a log in which this transaction is recorded.

The client is capable of doing the following: 
1. Transfers money to another client.
2. Sends an ”application” message to another client.
3. Checks its own balance.
In the first case (money transfer), the client only updates its local copy of the blockchain. In the second case (when sending a message), the client will transmit its local blockchain and the 2-dim TimeTable. The recipient client will update its local blockchain and balance table according to what it receives from the sender. In the last case (checking balance), the client responds based on it local balance table.
