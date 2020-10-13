import { ChannelCredentials, ClientDuplexStream, Metadata } from 'grpc';
import { ErrorMessage, FlowControl, SerializedObject } from './proto/common_pb';
import { ClientIdentification } from './proto/control_pb';
import { CommandServiceClient } from './proto/command_grpc_pb';
import { fromJson, toJson } from './serialize';
import { v4 as generateUUID } from 'uuid';

import {
  Command,
  CommandProviderInbound,
  CommandProviderOutbound,
  CommandResponse,
  CommandSubscription,
} from './proto/command_pb';

import { createProcessingInstruction } from './processingInstruction';
import { AnyFunction, Unsubscriber } from './types';

interface Options {
  endpoint: string;
  meta: Metadata;
  credentials: ChannelCredentials;
  clientIdentification: ClientIdentification;
}

export interface DispatchOptions<T = any> {
  name: string;
  payload?: {
    type: string;
    revision?: string;
    data: T;
  };
}

export interface DispatchResponse<T = any> {
  messageIdentifier: string;
  requestIdentifier: string;
  payload: {
    type: string;
    revision: string;
    data: T;
  };
}

export class CommandBus {
  private commandClient: CommandServiceClient;
  private meta: Metadata;
  private clientIdentification: ClientIdentification;
  private commandStream!: ClientDuplexStream<
    CommandProviderOutbound,
    CommandProviderInbound
  >;
  private subscriptions: Record<
    string,
    { responseType: string; operation: AnyFunction }
  > = {};
  private permits = 500;

  constructor({ meta, credentials, endpoint, clientIdentification }: Options) {
    this.commandClient = new CommandServiceClient(endpoint, credentials);

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
    this.commandStream = this.commandClient.openStream(this.meta);

    this.commandStream.on('data', async (d) => {
      const inbound = Object.assign(
        new CommandProviderInbound(),
        d
      ) as CommandProviderInbound;

      if (!inbound.hasCommand()) {
        return;
      }

      const command = inbound.getCommand()!.toObject();

      const subscription = this.subscriptions[command.name];

      if (!subscription) {
        return;
      }

      const outbound = new CommandProviderOutbound();
      const response = new CommandResponse();
      response.setRequestIdentifier(command.messageIdentifier);

      let reply;
      try {
        reply = subscription.operation(
          command.payload ? fromJson(command.payload.data as string) : undefined
        );

        if ('then' in reply) {
          reply = await reply;
        }
      } catch (e) {
        const err = new ErrorMessage();
        err.setMessage(e);
        response.setErrorMessage(err);
        outbound.setCommandResponse(response);
        this.commandStream.write(outbound);
        return;
      }

      if (reply) {
        const payload = new SerializedObject();
        payload.setType(subscription.responseType);
        payload.setRevision('');
        payload.setData(toJson(reply));
        response.setPayload(payload);
      }

      outbound.setCommandResponse(response);

      this.commandStream.write(outbound);
    });

    this.commandStream.on('error', (err) => console.error(err));

    const openResponse = new CommandProviderOutbound();
    const flowControl = new FlowControl();
    flowControl.setClientId(this.clientIdentification.getClientId());
    flowControl.setPermits(this.permits);
    openResponse.setFlowControl(flowControl);

    this.commandStream.write(openResponse);
  }

  subscribe(
    commandName: string,
    operation: AnyFunction,
    responseType: string
  ): Unsubscriber {
    if (this.subscriptions[commandName]) {
      throw new Error(
        `There is already a command-subscription for '${commandName}'`
      );
    }

    if (!this.commandStream) {
      this.openStream();
    }

    this.subscriptions[commandName] = {
      operation,
      responseType,
    };

    const outbound = new CommandProviderOutbound();
    const subscription = new CommandSubscription();
    subscription.setCommand(commandName);
    subscription.setClientId(this.clientIdentification.getClientId());
    subscription.setComponentName(this.clientIdentification.getComponentName());
    outbound.setSubscribe(subscription);
    this.commandStream.write(outbound);

    return () => this.unsubscribe(commandName);
  }

  unsubscribe(commandName: string) {
    if (!this.commandStream) {
      return;
    }

    if (!this.subscriptions[commandName]) {
      return;
    }

    const outbound = new CommandProviderOutbound();
    const subscription = new CommandSubscription();
    subscription.setCommand(commandName);
    subscription.setClientId(this.clientIdentification.getClientId());
    subscription.setComponentName(this.clientIdentification.getComponentName());
    outbound.setUnsubscribe(subscription);
    this.commandStream.write(outbound);
    delete this.subscriptions[commandName];
  }

  dispatch<T = any>({ name, payload }: DispatchOptions) {
    const request = new Command();
    request.setClientId(this.clientIdentification.getClientId());
    request.setComponentName(this.clientIdentification.getComponentName());
    request.setMessageIdentifier(generateUUID());
    request.setTimestamp(Date.now());
    request.setName(name);
    request.addProcessingInstructions(
      createProcessingInstruction('ROUTING_KEY', 0)
    );
    if (payload) {
      const p = new SerializedObject();
      p.setType(payload.type);
      p.setRevision(payload.revision ?? '');
      p.setData(toJson(payload.data));
      request.setPayload(p);
    }

    return new Promise<DispatchResponse<T>>((resolve, reject) => {
      this.commandClient.dispatch(request, this.meta, (err, response) => {
        if (err) {
          reject(err);
          return;
        }

        const {
          errorCode,
          metaDataMap,
          processingInstructionsList,
          errorMessage,
          ...data
        } = response!.toObject();

        const result: DispatchResponse = {
          ...data,
          payload: data.payload
            ? {
                ...data.payload,
                data: fromJson(data.payload.data as string),
              }
            : null!,
        };

        resolve(result);
      });
    });
  }

  close() {
    this.commandStream?.destroy();
    this.commandClient.close();
  }
}
