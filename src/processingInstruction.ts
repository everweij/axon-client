import {
  MetaDataValue,
  ProcessingInstruction,
  ProcessingKey,
  ProcessingKeyMap,
} from './proto/common_pb';

export function createProcessingInstruction(
  key: keyof ProcessingKeyMap,
  value: number
) {
  const instruction = new ProcessingInstruction();
  instruction.setKey(ProcessingKey[key]);
  const val = new MetaDataValue();
  val.setNumberValue(value);
  instruction.setValue(val);
  return instruction;
}
