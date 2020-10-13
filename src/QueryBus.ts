import { ChannelCredentials, ClientDuplexStream, Metadata } from 'grpc';
import { ErrorMessage, FlowControl, SerializedObject } from './proto/common_pb';
import { ClientIdentification } from './proto/control_pb';
import { QueryServiceClient } from './proto/query_grpc_pb';
import {
  QueryComplete,
  QueryProviderInbound,
  QueryProviderOutbound,
  QueryRequest,
  QueryResponse,
  QuerySubscription,
} from './proto/query_pb';
import { fromJson, toJson, toXml } from './serialize';
import { v4 as generateUUID } from 'uuid';

import { readStreamToPromise } from './grpcHelpers';
import { createProcessingInstruction } from './processingInstruction';
import { AnyFunction, Unsubscriber } from './types';

export interface QueryOptions {
  timeout?: number | null;
  priority?: number | null;
  nrOfResults?: number | null;
  query: string;
  payload?: {
    type?: string;
    revision?: string;
    data?: any;
  };
  responseType: {
    type?: string;
    revision?: string;
    data?: any;
  };
}

export interface QueryResult<T = any> {
  messageIdentifier: string;
  requestIdentifier: string;
  payload: {
    type: string;
    revision: string;
    data: T;
  };
}

interface Options {
  endpoint: string;
  meta: Metadata;
  credentials: ChannelCredentials;
  clientIdentification: ClientIdentification;
}

export class QueryBus {
  private queryClient: QueryServiceClient;
  private meta: Metadata;
  private clientIdentification: ClientIdentification;
  private queryStream!: ClientDuplexStream<
    QueryProviderOutbound,
    QueryProviderInbound
  >;
  private subscriptions: Record<
    string,
    { responseType: string; operation: AnyFunction }
  > = {};
  private permits = 500;

  constructor({ meta, credentials, endpoint, clientIdentification }: Options) {
    this.queryClient = new QueryServiceClient(endpoint, credentials);

    this.meta = meta;
    this.clientIdentification = clientIdentification;
  }

  getPermits() {
    return this.permits;
  }

  setPermits(permits: number) {
    this.permits = permits;
  }

  private openStream() {
    this.queryStream = this.queryClient.openStream(this.meta);

    this.queryStream.on('data', async (d) => {
      const inbound = Object.assign(
        new QueryProviderInbound(),
        d
      ) as QueryProviderInbound;

      if (!inbound.hasQuery()) {
        return;
      }

      const query = inbound.getQuery()!.toObject();

      const subscription = this.subscriptions[query.query];

      if (!subscription) {
        return;
      }

      const outbound = new QueryProviderOutbound();
      const response = new QueryResponse();
      response.setRequestIdentifier(query.messageIdentifier);

      let reply;
      try {
        reply = subscription.operation(
          query.payload ? fromJson(query.payload.data as string) : undefined
        );

        if ('then' in reply) {
          reply = await reply;
        }
      } catch (e) {
        const err = new ErrorMessage();
        err.setMessage(e);
        response.setErrorMessage(err);
        outbound.setQueryResponse(response);
        this.queryStream.write(outbound);
        return;
      }
      const messageId = generateUUID();
      response.setMessageIdentifier(messageId);

      if (reply) {
        const payload = new SerializedObject();
        payload.setType(subscription.responseType);
        payload.setRevision('');
        payload.setData(toJson(reply));
        response.setPayload(payload);
      }

      outbound.setQueryResponse(response);
      this.queryStream.write(outbound);

      const complete = new QueryComplete();
      complete.setRequestId(query.messageIdentifier);
      complete.setMessageId(messageId);
      outbound.setQueryComplete(complete);
      this.queryStream.write(outbound);
    });

    this.queryStream.on('end', (d: any) => console.log('end', d));

    this.queryStream.on('error', (err) => console.error(err));

    const openResponse = new QueryProviderOutbound();
    const flowControl = new FlowControl();
    flowControl.setClientId(this.clientIdentification.getClientId());
    flowControl.setPermits(this.permits);
    openResponse.setFlowControl(flowControl);

    this.queryStream.write(openResponse);
  }

  subscribe(
    queryName: string,
    operation: AnyFunction,
    responseType: string
  ): Unsubscriber {
    if (this.subscriptions[queryName]) {
      throw new Error(
        `There is already a query-subscription for '${queryName}'`
      );
    }

    if (!this.queryStream) {
      this.openStream();
    }

    this.subscriptions[queryName] = {
      operation,
      responseType,
    };

    const outbound = new QueryProviderOutbound();
    const subscription = new QuerySubscription();
    subscription.setQuery(queryName);
    subscription.setResultName(responseType);
    subscription.setClientId(this.clientIdentification.getClientId());
    subscription.setComponentName(this.clientIdentification.getComponentName());
    outbound.setSubscribe(subscription);
    this.queryStream.write(outbound);

    return () => this.unsubscribe(queryName);
  }

  unsubscribe(queryName: string) {
    if (!this.queryStream) {
      return;
    }

    if (!this.subscriptions[queryName]) {
      return;
    }

    const outbound = new QueryProviderOutbound();
    const subscription = new QuerySubscription();
    subscription.setQuery(queryName);
    subscription.setClientId(this.clientIdentification.getClientId());
    subscription.setComponentName(this.clientIdentification.getComponentName());
    outbound.setUnsubscribe(subscription);
    this.queryStream.write(outbound);
    delete this.subscriptions[queryName];
  }

  query<T = any>({
    query,
    timeout = null,
    priority = null,
    nrOfResults = null,
    responseType,
    payload,
  }: QueryOptions) {
    const request = new QueryRequest();
    request.setClientId(this.clientIdentification.getClientId());
    request.setComponentName(this.clientIdentification.getComponentName());

    if (timeout !== null) {
      request.addProcessingInstructions(
        createProcessingInstruction('TIMEOUT', timeout)
      );
    }
    if (priority !== null) {
      request.addProcessingInstructions(
        createProcessingInstruction('PRIORITY', priority)
      );
    }
    if (nrOfResults !== null) {
      request.addProcessingInstructions(
        createProcessingInstruction('NR_OF_RESULTS', nrOfResults)
      );
    }

    request.addProcessingInstructions(
      createProcessingInstruction('ROUTING_KEY', 0)
    );

    request.setQuery(query);
    request.setMessageIdentifier(generateUUID());
    request.setTimestamp(Date.now());

    if (payload) {
      const p = new SerializedObject();
      p.setType(payload.type ?? '');
      p.setRevision(payload.revision ?? '');
      p.setData(toJson(payload.data ?? {}));
      request.setPayload(p);
    }

    const r = new SerializedObject();
    r.setType(responseType.type ?? '');
    r.setRevision(responseType.revision ?? '');
    r.setData(responseType.data);
    request.setResponseType(r);

    return readStreamToPromise<QueryResult<T>, QueryResponse>(
      () => this.queryClient.query(request, this.meta),
      QueryResponse,
      null!,
      (_, payload) => {
        if (payload.hasErrorMessage()) {
          const message = payload.getErrorMessage();
          throw new Error(JSON.stringify(message?.toObject(), null, 4));
        }

        const {
          errorCode,
          metaDataMap,
          errorMessage,
          processingInstructionsList,
          ...data
        } = payload.toObject();

        const result: QueryResult<any> = {
          ...data,
          payload:
            data.payload && data.payload.data
              ? {
                  ...data.payload,
                  data: fromJson(data.payload.data as string),
                }
              : (undefined as any),
        };

        return result;
      }
    );
  }

  queryJava<T = any>({
    query,
    queryType = query,
    timeout,
    payload: payloadData,
    responseType,
    responseIsList = false,
    nrOfResults,
    priority,
  }: Omit<
    QueryOptions,
    'payload' | 'responseType' | 'messageIdentifier' | 'timestamp'
  > & {
    queryType?: string;
    payload: any;
    responseType: string;
    responseIsList?: boolean;
  }) {
    const responseClass = `org.axonframework.messaging.responsetypes.${
      responseIsList ? 'MultipleInstancesResponseType' : 'InstanceResponseType'
    }`;

    return this.query<T>({
      query,
      timeout,
      nrOfResults,
      priority,
      payload: {
        type: queryType,
        data: payloadData,
      },
      responseType: {
        type: responseClass,
        data: toXml({
          [responseClass]: {
            expectedResponseType: responseType,
          },
        }),
      },
    });
  }

  close() {
    this.queryStream?.destroy();
    this.queryClient.close();
  }
}
