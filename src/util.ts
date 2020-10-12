import { inspect } from 'util';

export function logData(data: any) {
  console.log(inspect(data, false, null, true));
}
