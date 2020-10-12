# axon-client

[![NPM](https://img.shields.io/npm/v/axon-client.svg)](https://www.npmjs.com/package/axon-client)
[![TYPESCRIPT](https://img.shields.io/badge/%3C%2F%3E-typescript-blue)](http://www.typescriptlang.org/)
[![Weekly downloads](https://badgen.net/npm/dw/axon-client)](https://badgen.net/npm/dw/axon-client)

Unofficial NodeJS client for Axon Server written in TypeScript.
Note: This library has a peer-dependency on `rxjs >= 6`.

## Installation

Via npm

```bash
npm install axon-client
```

or yarn

```bash
yarn add axon-client
```

## Getting started

In order to setup a connection:

```ts
import { AxonClient } from 'axon-client';

async function app() {
  const client = await new AxonClient({
    componentName: 'My-App',
    clientId: '123@hostname', // optional, defaults to process-id + hostname
    host: '0.0.0.0',
    port: 8124, // optional , defaults to 8124
    token: 'my-secret-token', // optional
    certificate: fs.readFileSync('path-to-certificate'), // optional
  }).connect();

  console.log(`Connected to Axon Server on ${client.endpoint}`);

  // do stuff with the client here...

  // optionally disconnect when done
  client.disconnect();
}
```

## Methods

The following methods are available.

### eventBus

```ts
queryEvents(query: string, numberOfPermits?: number): Observable<QueryEvents>;
getFirstToken(): Promise<number>;
getLastToken(): Promise<number>;
getTokenAt(instant: number): Promise<number>;
listAggregateEvents(options: ListAggregateEventsOptions): Promise<AggregateEvent[]>;
listAggregateSnapshots(options: ListAggregateSnapshotsOptions): Promise<any[]>;
listEvents(options: ListEventOptions): Observable<Event>;
appendEvents(event: AggregateEvent | AggregateEvent[]): Promise<Confirmation>;
```

### commandBus

```ts
getPermits(): number;
setPermits(permits: number): void;
subscribe(commandName: string, operation: AnyFunction): Unsubscriber;
unsubscribe(commandName: string): void;
dispatch({ name, payload }: DispatchOptions): Promise<DispatchResponse>;
```

### queryBus

Unfortunately, query-subscription are not (yet) possible.

```ts
getPermits(): number;
setPermits(permits: number): void;
subscribe(queryName: string, operation: AnyFunction): Unsubscriber;
unsubscribe(queryName: string): void;
query(options: QueryOptions): Promise<QueryResult>;
```
