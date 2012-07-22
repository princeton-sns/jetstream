


proto:
	protoc worker_api.proto --cpp_out=src/gen_cpp	

clean:
	rm src/gen_cpp/*
