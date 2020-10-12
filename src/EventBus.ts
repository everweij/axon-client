import { ChannelCredentials, ClientDuplexStream, Metadata } from 'grpc';
import { EventStoreClient } from './proto/event_grpc_pb';
import { map, filter } from 'rxjs/operators';
import {
  EventWithToken,
  GetAggregateEventsRequest,
  GetEventsRequest,
  GetFirstTokenRequest,
  GetLastTokenRequest,
  GetTokenAtRequest,
  QueryEventsRequest,
  QueryEventsResponse,
  Event as AEvent,
  GetAggregateSnapshotsRequest,
  PayloadDescription,
  Confirmation,
} from './proto/event_pb';
import { fromJson, toJson } from './serialize';
import { duplexStreamToObservable, readStreamToPromise } from './grpcHelpers';
import { SerializedObject } from './proto/common_pb';

export interface EventBase<Type = string, Data = any> {
  aggregateIdentifier: string;
  aggregateSequenceNumber: number;
  aggregateType: string;
  timestamp: number;
  payload?: {
    type: Type;
    revision: string;
    data: Data;
  };
}

export interface AggregateEvent<Type = string, Data = any>
  extends EventBase<Type, Data> {
  messageIdentifier: string;
  snapshot?: boolean;
}

export interface Event<Type = string, Data = any>
  extends EventBase<Type, Data> {
  token: number;
}

interface Options {
  endpoint: string;
  meta: Metadata;
  credentials: ChannelCredentials;
}

export interface ListAggregateEventsOptions {
  aggregateId: string;
  /* Default is true */
  allowSnapshots?: boolean;
  /* Default is 0 */
  initialSequence?: number;
  /* Default is 0 */
  trackingToken?: number;
  /* Default is -1 */
  maxSequence?: number;
}

export interface ListAggregateSnapshotsOptions {
  aggregateId: string;
  /* Default is 0 */
  initialSequence?: number;
  /* Default is -1 */
  maxSequence?: number;
}

export interface BlacklistItem {
  type: string;
  revision?: string;
}

export interface ListEventOptions {
  blacklistItems?: BlacklistItem[];
  clientId?: string;
  componentName?: string;
  forceReadFromLeader?: boolean;
  processor?: string;
  trackingToken?: number;
  numberOfPermits?: number;
}

export type NumericQueryOperator =
  | { $lt: number }
  | { $lte: number }
  | { $gt: number }
  | { $gte: number }
  | { $eq: number };

export interface QueryEventsOptions {
  token?: number | NumericQueryOperator;
  aggregateIdentifier?: string;
  aggregateSequenceNumber?: number | NumericQueryOperator;
  aggregateType?: string;
  payloadType?: string;
  payloadRevision?: string;
  timestamp?: number | NumericQueryOperator;
  numberOfPermits?: number;
}

export interface QueryItem<T = any> {
  token: number;
  aggregateIdentifier: string;
  aggregateSequenceNumber: number;
  aggregateType: string;
  eventIdentifier: string;
  payload?: {
    type: string;
    revision: string;
    data: T;
  };
  timestamp: number;
}

const NumericOperatorStringMap = {
  $lt: '<',
  $lte: '<=',
  $gt: '>',
  $gte: '>=',
  $eq: '=',
};

function createNumericQuery(key: string, value: number | NumericQueryOperator) {
  if (typeof value === 'number') {
    return `${key} = ${value}`;
  }

  for (const [operator, stringValue] of Object.entries(
    NumericOperatorStringMap
  )) {
    if (operator in value) {
      return `${key} ${stringValue} ${(value as any)[operator]}`;
    }
  }

  return '';
}

function createTextQuery({
  aggregateIdentifier,
  aggregateSequenceNumber,
  aggregateType,
  payloadRevision,
  payloadType,
  timestamp,
  token,
}: QueryEventsOptions) {
  const list: string[] = [];

  if (aggregateIdentifier) {
    list.push(`aggregateIdentifier = "${aggregateIdentifier}"`);
  }
  if (aggregateType) {
    list.push(`aggregateType = "${aggregateType}"`);
  }
  if (payloadRevision) {
    list.push(`payloadRevision = "${payloadRevision}"`);
  }
  if (payloadType) {
    list.push(`payloadType = "${payloadType}"`);
  }
  if (timestamp !== undefined) {
    list.push(createNumericQuery('timestamp', timestamp));
  }
  if (aggregateSequenceNumber !== undefined) {
    list.push(
      createNumericQuery('aggregateSequenceNumber', aggregateSequenceNumber)
    );
  }
  if (token !== undefined) {
    list.push(createNumericQuery('token', token));
  }

  if (list.length === 0) {
    throw new Error(
      'In order to query events you must provide at least one key'
    );
  }

  return list.join(' AND ');
}

export class EventBus {
  private eventClient: EventStoreClient;
  private meta: Metadata;
  private currentEventStream: ClientDuplexStream<any, any> | null = null;

  constructor({ endpoint, credentials, meta }: Options) {
    this.eventClient = new EventStoreClient(endpoint, credentials);
    this.meta = meta;
  }

  queryEvents(options: QueryEventsOptions) {
    const call = this.eventClient.queryEvents(this.meta);
    const request = new QueryEventsRequest();
    const query = createTextQuery(options);
    request.setQuery(query);

    return duplexStreamToObservable(
      call,
      request,
      QueryEventsResponse,
      options.numberOfPermits || 100
    ).pipe(
      filter((value) => value.hasRow()),
      map((value) => {
        const { row } = value.toObject();

        const result: QueryItem = {} as any;

        for (const [key, values] of row!.valuesMap) {
          if (key === 'aggregateIdentifier') {
            result.aggregateIdentifier = values.textValue;
          }
          if (key === 'aggregateSequenceNumber') {
            result.aggregateSequenceNumber = values.numberValue;
          }
          if (key === 'aggregateType') {
            result.aggregateType = values.textValue;
          }
          if (key === 'eventIdentifier') {
            result.eventIdentifier = values.textValue;
          }
          if (key === 'payloadData') {
            result.payload = result.payload || ({} as any);
            result.payload!.data = JSON.parse(values.textValue);
          }
          if (key === 'payloadRevision') {
            result.payload = result.payload || ({} as any);
            result.payload!.revision = values.textValue;
          }
          if (key === 'payloadType') {
            result.payload = result.payload || ({} as any);
            result.payload!.type = values.textValue;
          }
          if (key === 'timestamp') {
            result.timestamp = values.numberValue;
          }
          if (key === 'token') {
            result.token = values.numberValue;
          }
        }

        return result;
      })
    );
  }

  getFirstToken() {
    return new Promise<number>((resolve, reject) => {
      this.eventClient.getFirstToken(
        new GetFirstTokenRequest(),
        this.meta,
        (err, payload) => {
          if (err) {
            reject(err);
          } else {
            resolve(payload!.getToken());
          }
        }
      );
    });
  }

  getLastToken() {
    return new Promise<number>((resolve, reject) => {
      this.eventClient.getLastToken(
        new GetLastTokenRequest(),
        this.meta,
        (err, payload) => {
          if (err) {
            reject(err);
          } else {
            resolve(payload!.getToken());
          }
        }
      );
    });
  }

  getTokenAt(instant: number) {
    const request = new GetTokenAtRequest();
    request.setInstant(instant);

    return new Promise<number>((resolve, reject) => {
      this.eventClient.getTokenAt(request, this.meta, (err, payload) => {
        if (err) {
          reject(err);
        } else {
          resolve(payload!.getToken());
        }
      });
    });
  }

  listAggregateEvents({
    aggregateId,
    allowSnapshots,
    initialSequence,
    trackingToken,
    maxSequence,
  }: ListAggregateEventsOptions) {
    const request = new GetAggregateEventsRequest();
    request.setAggregateId(aggregateId);
    if (allowSnapshots) {
      request.setAllowSnapshots(allowSnapshots);
    }
    if (initialSequence) {
      request.setInitialSequence(initialSequence);
    }
    if (trackingToken) {
      request.setMinToken(trackingToken);
    }
    if (maxSequence) {
      request.setMaxSequence(maxSequence);
    }

    return readStreamToPromise<AggregateEvent[], AEvent>(
      () => this.eventClient.listAggregateEvents(request, this.meta),
      AEvent,
      [],
      (data, event) => {
        const obj = event.toObject();
        const item: AggregateEvent = {
          ...obj,
          payload: {
            ...obj.payload!,
            data: fromJson(obj.payload!.data as string),
          },
        };

        return [...data, item];
      }
    );
  }

  listAggregateSnapshots({
    aggregateId,
    initialSequence,
    maxSequence,
  }: ListAggregateSnapshotsOptions) {
    const request = new GetAggregateSnapshotsRequest();
    request.setAggregateId(aggregateId);
    if (initialSequence) {
      request.setInitialSequence(initialSequence);
    }
    if (maxSequence) {
      request.setMaxSequence(maxSequence);
    }

    return readStreamToPromise<any[], AEvent>(
      () => this.eventClient.listAggregateSnapshots(request, this.meta),
      AEvent,
      [],
      (data, event) => {
        const obj = event.toObject();
        const item: AggregateEvent = {
          ...obj,
          payload: {
            ...obj.payload!,
            data: fromJson(obj.payload!.data as string),
          },
        };

        return [...data, item];
      }
    );
  }

  listEvents({
    blacklistItems,
    clientId,
    componentName,
    forceReadFromLeader,
    processor,
    trackingToken,
    numberOfPermits = 10,
  }: ListEventOptions = {}) {
    const request = new GetEventsRequest();

    if (blacklistItems) {
      request.setBlacklistList(
        blacklistItems.map(({ type, revision }) => {
          const description = new PayloadDescription();
          description.setType(type);
          description.setRevision(revision ?? '');
          return description;
        })
      );
    }
    if (clientId) {
      request.setClientId(clientId);
    }
    if (componentName) {
      request.setComponentName(componentName);
    }
    if (forceReadFromLeader) {
      request.setForceReadFromLeader(forceReadFromLeader);
    }
    if (processor) {
      request.setProcessor(processor);
    }
    if (trackingToken) {
      request.setTrackingToken(trackingToken);
    }

    return duplexStreamToObservable(
      this.eventClient.listEvents(this.meta),
      request,
      EventWithToken,
      numberOfPermits
    ).pipe(
      map((event): Event | null => {
        const obj = event.toObject();

        if (!obj.event) {
          return null;
        }

        return {
          token: obj.token,
          aggregateIdentifier: obj.event.aggregateIdentifier,
          aggregateSequenceNumber: obj.event.aggregateSequenceNumber,
          aggregateType: obj.event.aggregateType,
          timestamp: obj.event.timestamp,
          payload: {
            ...obj.event.payload!,
            data: fromJson(obj.event.payload!.data as string),
          },
        };
      })
    );
  }

  appendEvents(
    event: AggregateEvent | AggregateEvent[]
  ): Promise<Confirmation> {
    const events = (Array.isArray(event) ? event : [event]).map(
      ({
        aggregateIdentifier,
        aggregateSequenceNumber,
        aggregateType,
        messageIdentifier,
        payload,
        timestamp,
      }) => {
        const event = new AEvent();
        event.setAggregateIdentifier(aggregateIdentifier);
        event.setAggregateSequenceNumber(aggregateSequenceNumber);
        event.setAggregateType(aggregateType);
        event.setMessageIdentifier(messageIdentifier);

        if (payload) {
          const p = new SerializedObject();
          p.setType(payload.type);
          p.setRevision(payload.revision);
          p.setData(toJson(payload.data));
          event.setPayload(p);
        }

        event.setTimestamp(timestamp);

        return event;
      }
    );

    return new Promise((resolve, reject) => {
      const call = this.eventClient.appendEvent(this.meta, (err, response) => {
        if (err) {
          reject(err);
        } else {
          resolve(response);
        }
      });

      for (const event of events) {
        call.write(event);
      }

      call.end();
    });
  }

  close() {
    if (this.currentEventStream) {
      this.currentEventStream.cancel();
    }
    this.eventClient.close();
  }
}
