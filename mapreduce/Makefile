linux:
		rm -rf release
		mkdir release
		pushd query-1/mapper && GO111MODULE=on GOOS=linux GOARCH=amd64 go build -o query_1_mapper . && popd && mv query-1/mapper/query_1_mapper release/
		pushd query-1/reducer && GO111MODULE=on GOOS=linux GOARCH=amd64 go build -o query_1_reducer . && popd && mv query-1/reducer/query_1_reducer release/
		pushd query-2/mapper && GO111MODULE=on GOOS=linux GOARCH=amd64 go build -o query_2_mapper . && popd && mv query-2/mapper/query_2_mapper release/
		pushd query-2/reducer && GO111MODULE=on GOOS=linux GOARCH=amd64 go build -o query_2_reducer . && popd && mv query-2/reducer/query_2_reducer release/
		pushd query-3/mapper && GO111MODULE=on GOOS=linux GOARCH=amd64 go build -o query_3_mapper . && popd && mv query-3/mapper/query_3_mapper release/
		pushd query-3/reducer && GO111MODULE=on GOOS=linux GOARCH=amd64 go build -o query_3_reducer . && popd && mv query-3/reducer/query_3_reducer release/


darwin:
		rm -rf release
		mkdir release
		pushd query-1/mapper && go build -o query_1_mapper . && popd && mv query-1/mapper/query_1_mapper release/
		pushd query-1/reducer && go build -o query_1_reducer . && popd && mv query-1/reducer/query_1_reducer release/
		pushd query-2/mapper && go build -o query_2_mapper . && popd && mv query-2/mapper/query_2_mapper release/
		pushd query-2/reducer && go build -o query_2_reducer . && popd && mv query-2/reducer/query_2_reducer release/
		pushd query-3/mapper && go build -o query_3_mapper . && popd && mv query-3/mapper/query_3_mapper release/
		pushd query-3/reducer && go build -o query_3_reducer . && popd && mv query-3/reducer/query_3_reducer release/
