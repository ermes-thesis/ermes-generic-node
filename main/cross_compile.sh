sudo apt-get update 
sudo apt-get install -y build-essential gcc-aarch64-linux-gnu g++-aarch64-linux-gnu xz-utils libssl-dev zlib1g-dev:arm64
wget https://github.com/WasmEdge/WasmEdge/releases/download/0.16.3/WasmEdge-0.16.3-manylinux_2_28_aarch64.tar.gz
mkdir -p ./WasmEdge-0.16.3-aarch64
tar -xf WasmEdge-0.16.3-manylinux_2_28_aarch64.tar.gz -C WasmEdge-0.16.3-aarch64
mkdir -p ./wasmedge_libs_arm64
cp -r WasmEdge-0.16.3-aarch64/lib64/* ./wasmedge_libs_arm64/
sudo cp -r WasmEdge-0.16.3-aarch64/include/* /usr/local/include/
rm -rf WasmEdge-0.16.3*
CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc GOOS=linux GOARCH=arm64 CGO_LDFLAGS="-L./wasmedge_libs_arm64 -lwasmedge" CGO_CFLAGS="-I/usr/local/include" go build -o main-arm64