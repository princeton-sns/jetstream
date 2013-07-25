#include "js_mt_shim.h"
#include "circular_int.hh"
#include "shared_config.hh"
#include <iostream>
#include <string>
#include "misc.hh"
#include "kvthread.hh"

using namespace std;

char k2_copy[30];

void ptr_test() {
  cout << "\n\n------------Ptr test---------------\n";
  JSMasstreePtr<char> tree;
  const char * mykey = "this is a key";
  const char * k2 = "k2";
  string val = "some data";
  const char * ptr = val.c_str();
  const char * ret;
  
  strcpy(k2_copy, k2);
  
  tree.set(k2, ptr);
  ret = tree.get(k2_copy);
  if (ret != ptr) {
    cout << "FAIL: got back something I didn't put in: " << (void*)ptr << " versus "<< (void*)ret << "\n";
  }
  else
    cout << "OK (pointing to valid memory, using byte-identical keys at distinct addresses)\n";
  
  tree.set(mykey, (char *) 17);
  ret = tree.get(mykey);
  const void * addr = reinterpret_cast<const void *>(ret);
  if ( reinterpret_cast<long>(addr) == 17)
    cout << "OK got back " << addr << endl;
  else
    cout << "FAIL, got back " << addr << endl;
//  cout << "OK (invalid memory)\n";
}

void base_test() {
  cout << "\n--------------Base test-------------\n";

  JSMasstree tree;

  string mykey = "this is a key";
  string val = "this is a value";
//  int val = 17;
  
//  tree.set(mykey.c_str(), (char*) &val, sizeof(int));
  cout << "str addr is " << (void *)val.c_str() << endl;
  tree.set(mykey.c_str(), val.c_str(), val.length());
//  tree.set(mykey.c_str(), val.c_str(), val.length());

  int els = tree.elements();
  cout << "set, leaving " << els << " elements" << endl;
  
//  int ret = 0;
  char ret[100];
  memset(ret, 0, sizeof(ret));
  size_t sz =  sizeof(ret);
  tree.get(mykey.c_str(), (char*) ret, sz);
  cout <<"Got " << ret << " (" << sz << " bytes)";
  if (ret == val && sz == val.length())
    cout << "...OK, result buffer is identical"<<endl;
  else
    cout << "...FAIL. Expected " << val << endl;
  
  
  tree.clear();
  if (tree.elements() == 0)
    cout << "OK: cleared" <<endl;
  else
    cout << "FAIL: trying to clear, size was " << tree.elements() << endl;
  
  long v1 = 2;
  long v2 = 3;
  tree.set("key1", (char*)&v1, sizeof(v1));
  tree.set("key2", (char*) &v2, sizeof(v2));
  long r1 = 0;
  size_t r_sz = sizeof(r1);
  tree.get("key1", (char *) &r1, r_sz);
  
  if (v1 == r1 && r_sz == sizeof(r1))
    cout << "reading raw int, OK\n";
  else
    cout << "FAIL: put " << v1 << " and got " << r1 << "(" << r_sz << ")"<< endl;
}

void scan_test() {

  cout << "\n--------------Scan test-------------\n";

  JSMasstree tree;

  long v[2] = {2,3};
  
  tree.set("key2", (const char*) (&v[0]), sizeof(v[0]));
  tree.set("key3", (char*) (v + 1), sizeof(v[0]));

  JSMScanner myscanner = tree.scan("key1", "key4");
  
  for (int i = 0; i < 2; ++i) {
    if (!myscanner.hasNext()) {
      cout << "FAIL: Expected a next\n";
    } else {
      long r1 = 0;
      string mykey;
      size_t len = 8;
      myscanner.next(mykey, reinterpret_cast<char *>(&r1), len);
      if (len < 8) {
        cout<< "FAIL Expected len = 8, got " << len <<endl;
      }
      if (mykey != "key2") {
        cout << "FAIL: Expected key key2, got " << mykey << endl;
      }
      if(r1 != v[i])
        cout << "FAIL, got " << r1 << " and expected 2\n";
      else
        cout << "OK (element"<< i << ")\n";
    }
  }
}

int main() {
  cout << "started" <<endl;
  
  base_test();
  ptr_test();
  scan_test();
  cout << "done" << endl;



  return 0;
}