const { mkdirSync } = require('fs');
const { execSync } = require('child_process');
const path = require('path');

const APP_FOLDER = path.resolve(__dirname, '../');
const BASE = path.join(
  APP_FOLDER,
  'node_modules/axon-server-api/src/main/proto'
);
const OUT = path.join(APP_FOLDER, 'src', 'proto');

function getBinaryFile(name) {
  return path.join(APP_FOLDER, 'node_modules/.bin', name);
}

execSync(`rm -rf ${OUT}`);
mkdirSync(OUT);

execSync(`
${getBinaryFile('grpc_tools_node_protoc')} \
--js_out=import_style=commonjs,binary:${OUT} \
--grpc_out=${OUT} \
--plugin=protoc-gen-grpc='${getBinaryFile('grpc_tools_node_protoc_plugin')}' \
--proto_path=${BASE} \
${BASE}/*.proto
`);

execSync(`
${getBinaryFile('grpc_tools_node_protoc')} \
--plugin=protoc-gen-ts=${getBinaryFile('protoc-gen-ts')} \
--ts_out=service=grpc-node:${OUT} \
-I ${BASE} \
${BASE}/*.proto
`);
