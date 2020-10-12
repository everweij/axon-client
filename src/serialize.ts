import { js2xml } from 'xml-js';

export function toXml(data: Record<string, any>) {
  let xml = js2xml(data, { compact: true, ignoreComment: true, spaces: 4 });
  return Buffer.from(xml);
}

export function fromJson(base64: string) {
  return JSON.parse(Buffer.from(base64, 'base64').toString('utf8'));
}

export function toJson(obj: any) {
  return Buffer.from(JSON.stringify(obj));
}
