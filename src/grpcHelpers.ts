import { ClientDuplexStream, ClientReadableStream } from 'grpc';
import { Observable } from 'rxjs';

export function duplexStreamToObservable<
  A extends { setNumberOfPermits(permits: number): void },
  B extends { toObject(): any }
>(
  call: ClientDuplexStream<A, B>,
  request: A,
  Response: new () => B,
  numberOfPermits: number
): Observable<B> {
  return new Observable<B>(observer => {
    let requests_before_new_permits = Math.floor(numberOfPermits / 2);
    request.setNumberOfPermits(numberOfPermits);

    call.on('data', d => {
      const data = Object.assign(new Response(), d) as B;
      observer.next(data);
      requests_before_new_permits--;
      if (requests_before_new_permits === 0) {
        call.write(request);
        requests_before_new_permits += numberOfPermits;
      }
    });
    call.on('error', error => {
      observer.error(error);
    });
    call.on('end', () => {
      observer.complete();
    });

    call.write(request);
  });
}

export function readStreamToPromise<A, B extends { toObject(): any }>(
  callThunk: () => ClientReadableStream<B>,
  Response: new () => B,
  initialData: A,
  transformData: (data: A, payload: B) => A
): Promise<A> {
  return new Promise<A>((resolve, reject) => {
    const call = callThunk();

    let data: A = initialData;
    let error: any;

    call.on('data', function(d) {
      const event = Object.assign(new Response(), d);
      try {
        data = transformData(data, event);
      } catch (e) {
        error = e;
      }
    });

    call.on('error', err => reject(err));

    call.on('end', () => {
      if (error) {
        reject(error);
      } else {
        resolve(data);
      }
    });
  });
}
