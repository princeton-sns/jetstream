


proto:
	protoc --proto_path=src/proto src/proto/worker_api.proto --cpp_out=src/gen_cpp --python_out=src/python/jetstream/gen/

clean:
	rm src/gen_cpp/* 
	rm src/python/jetstream/gen/*
