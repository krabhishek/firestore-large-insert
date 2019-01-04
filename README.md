# firestore-large-insert
Sample code for doing a large sequential insert into Firestore.

This program will store a large result set in memory, so please increase the default node memory size by running this code with following flag:

```console
node --max-old-space-size=4096 index.js > output500k.txt
```
