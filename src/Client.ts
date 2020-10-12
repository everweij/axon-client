import os from 'os';
import { ClientIdentification } from './proto/control_pb';
import { Metadata, credentials as cred, ChannelCredentials } from 'grpc';

import { Platform } from './Platform';
import { EventBus } from './EventBus';
import { QueryBus } from './QueryBus';
import { CommandBus } from './CommandBus';

function createClientIdentification(id: string, name: string) {
  const clientIdentification = new ClientIdentification();
  clientIdentification.setClientId(id);
  clientIdentification.setComponentName(name);
  return clientIdentification;
}

function createMeta(token?: string) {
  const meta = new Metadata();

  if (token) {
    meta.set('AxonIQ-Access-Token', token);
  }

  return meta;
}

interface Options {
  host: string;
  port?: number;
  certificate?: Buffer;
  token?: string;
  clientId?: string;
  componentName: string;
}

export class AxonClient {
  private platform: Platform;
  private credentials: ChannelCredentials;
  private meta: Metadata;
  private clientIdentification: ClientIdentification;

  readonly endpoint: string | null = null;
  readonly eventBus: EventBus = null!;
  readonly queryBus: QueryBus = null!;
  readonly commandBus: CommandBus = null!;

  constructor({
    clientId = process.pid + '@' + os.hostname(),
    componentName,
    host,
    certificate,
    port = 8124,
    token,
  }: Options) {
    const endpoint = `${host}:${port}`;
    const clientIdentification = createClientIdentification(
      clientId,
      componentName
    );
    const meta = createMeta(token);
    const credentials = certificate
      ? cred.createSsl(certificate)
      : cred.createInsecure();

    this.credentials = credentials;
    this.meta = meta;
    this.clientIdentification = clientIdentification;

    this.platform = new Platform({
      endpoint,
      meta,
      credentials,
      clientIdentification,
    });
  }

  async connect() {
    // @ts-ignore
    this.endpoint = await this.platform.getPlatformServer();

    // @ts-ignore
    this.eventBus = new EventBus({
      credentials: this.credentials,
      endpoint: this.endpoint,
      meta: this.meta,
    });

    // @ts-ignore
    this.queryBus = new QueryBus({
      credentials: this.credentials,
      endpoint: this.endpoint,
      meta: this.meta,
      clientIdentification: this.clientIdentification,
    });

    // @ts-ignore
    this.commandBus = new CommandBus({
      credentials: this.credentials,
      endpoint: this.endpoint,
      meta: this.meta,
      clientIdentification: this.clientIdentification,
    });

    return this;
  }

  disconnect() {
    this.eventBus?.close();
    this.queryBus?.close();
    this.commandBus?.close();
    this.platform.close();
  }
}
