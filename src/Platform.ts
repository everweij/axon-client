import { ChannelCredentials, ClientDuplexStream, Metadata } from 'grpc';
import { PlatformServiceClient } from './proto/control_grpc_pb';
import {
  ClientIdentification,
  PlatformInboundInstruction,
  PlatformOutboundInstruction,
} from './proto/control_pb';

interface Options {
  endpoint: string;
  meta: Metadata;
  credentials: ChannelCredentials;
  clientIdentification: ClientIdentification;
}

export class Platform {
  private client: PlatformServiceClient;
  private meta: Metadata;
  private endpoint: string;
  private clientIdentification: ClientIdentification;
  private credentials: ChannelCredentials;
  private controlStream!: ClientDuplexStream<
    PlatformInboundInstruction,
    PlatformOutboundInstruction
  >;

  constructor({ endpoint, meta, credentials, clientIdentification }: Options) {
    this.client = new PlatformServiceClient(endpoint, credentials);

    this.meta = meta;
    this.endpoint = endpoint;
    this.clientIdentification = clientIdentification;
    this.credentials = credentials;

    this.openStream();
  }

  getPlatformServer() {
    return new Promise<string>((resolve, reject) => {
      this.client.getPlatformServer(
        this.clientIdentification,
        this.meta,
        (err, platformInfo) => {
          if (err) {
            reject(err);
            return;
          }

          let endpoint: string;

          if (platformInfo!.getSameConnection()) {
            endpoint = this.endpoint;
          } else {
            const primary = platformInfo!.getPrimary()!;
            endpoint = `${primary.getHostName()}:${primary.getGrpcPort()}`;

            this.client = new PlatformServiceClient(endpoint, this.credentials);
          }

          this.client.openStream();

          resolve(endpoint);
        }
      );
    });
  }

  openStream() {
    const controlStream = this.client.openStream(this.meta);
    controlStream.on('data', function(d) {
      const data = Object.assign(
        new PlatformOutboundInstruction(),
        d
      ) as PlatformOutboundInstruction;

      console.info(data.toObject());
    });

    controlStream.on('error', function(err) {
      console.warn('Axon-Server control stream error: ', err);
    });

    controlStream.on('end', function() {
      console.warn('Axon-Server control stream ended');
    });

    const instruction = new PlatformInboundInstruction();
    instruction.setRegister(this.clientIdentification);

    controlStream.write(instruction);

    this.controlStream = controlStream;
  }

  close() {
    this.controlStream?.destroy();
    this.client.close();
  }
}
