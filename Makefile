

all:
	echo "'all' not yet implemented. Try make proto or make test"

proto:
	protoc --proto_path=src/proto src/proto/*.proto --cpp_out=src/gen_cpp --python_out=src/python/jetstream/gen/


test:
	bash src/python/run_python_tests.sh

clean:
	rm src/gen_cpp/* 
	rm src/python/jetstream/gen/*_pb2.py
