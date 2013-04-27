/**
 *  Tests for various system operators, such as ReadLine
 */

#include "base_operators.h"
#include "chain_ops.h"
#include <map>
#include <iostream>
#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>
#include <boost/regex.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include "node.h"
#include <gtest/gtest.h>

#include "experiment_operators.h"

using namespace jetstream;
using namespace boost;
using namespace std;

class COperatorTest : public ::testing::Test {
public:

  boost::shared_ptr<Node> node;
  virtual void SetUp() {
  
    if (!node) {
      NodeConfig cfg;
      cfg.heartbeat_time = 2000;
      boost::system::error_code err;
      node = shared_ptr<Node>(new Node(cfg, err));
    }
    node->start();
  }
  
  virtual void TearDown() {
    node->stop();
  }


};


TEST_F(COperatorTest, FileRead) {
  // constants describing the test data file
  enum {TEST_DATA_N_LINES = 19, TEST_DATA_N_EMPTY = 1};

  boost::shared_ptr<OperatorChain> chain(new OperatorChain);
  shared_ptr<CFileRead> reader(new CFileRead);
  map<string,string> config;
  config["file"] =  "src/tests/data/base_operators_data.txt";
  config["skip_empty"] = "false";
  config["exit_at_end"] = "true";
  reader->configure(config);
  reader->set_node(node.get());
  reader->add_chain(chain);
  
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  
  chain->add_member(reader);
  chain->add_member(rec);
  
  chain->start();
  
  // Wait for reader to process entire file (alternatively, call stop() after a
  // while)
  boost::this_thread::sleep(boost::posix_time::milliseconds(200));

  /*
  int waits = 0;
  while (reader.isRunning() && waits++ < 20) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(200));
  }
  ASSERT_GT (20, waits);*/
  chain->stop();

  ASSERT_GT(rec->tuples.size(), (size_t)4);
  ASSERT_EQ((size_t) TEST_DATA_N_LINES + 1, rec->tuples.size()); // file read adds blank line at end of file
  string s = rec->tuples[0]->e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
  ASSERT_NE(s[s.length() -1], '\n'); //check that we prune \n.


  // try again, with the option to skip 0-length lines turned on
  rec->tuples.clear();

  shared_ptr<CFileRead> reader2(new CFileRead);
  config["skip_empty"] = "true";
  reader2->configure(config);
  reader2->set_node(node.get());
  
  
  boost::shared_ptr<OperatorChain> chain2(new OperatorChain);
  chain2->add_member(reader2);
  chain2->add_member(rec);
  reader2->add_chain(chain2);
  
  
  chain2->start();
  boost::this_thread::sleep(boost::posix_time::milliseconds(200));

  chain2->stop();


  ASSERT_EQ((size_t) TEST_DATA_N_LINES - TEST_DATA_N_EMPTY, rec->tuples.size());
  ASSERT_GT(rec->tuples.size(), (size_t)4);
  s = rec->tuples[0]->e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
  ASSERT_NE(s[s.length() -1], '\n'); //check that we prune \n.
}


TEST(COperator, CExtendOperator) {

  shared_ptr<CExtendOperator>  ex_1(new CExtendOperator);
  shared_ptr<CExtendOperator> ex_host(new CExtendOperator);
  shared_ptr<DummyReceiver> rec(new DummyReceiver);

  operator_config_t cfg;
  cfg["types"] = "i";
  cfg["0"] = "1";
  ex_1->configure(cfg);

  cfg["types"] = "s";
  cfg["0"] = "${HOSTNAME}";
  ex_host->configure(cfg);

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);
  vector< boost::shared_ptr<Tuple> > v;
  v.push_back(t);
  boost::shared_ptr<OperatorChain> chain(new OperatorChain);
  
  boost::shared_ptr<COperator> no_op;
  chain->add_member(no_op);
  chain->add_member(ex_1);
  chain->add_member(ex_host);
  chain->add_member(rec);
  DataplaneMessage no_meta;
  chain->process(v, no_meta);

  ASSERT_EQ((size_t)1, rec->tuples.size());

  boost::shared_ptr<Tuple> result = rec->tuples[0];
  ASSERT_EQ(3, result->e_size());
  ASSERT_EQ(2, result->e(0).i_val()); //should preserve existing element[s]
  ASSERT_EQ(1, result->e(1).i_val()); //should preserve existing element[s]
  ASSERT_GT(result->e(2).s_val().length(), 2U);
  ASSERT_EQ( boost::asio::ip::host_name(), result->e(2).s_val());
  //  cout << "host name is "<< result->e(2).s_val() << endl;
  cout << "done" << endl;
}



TEST(Operator, CSVParseOperator) {
  map<string,string> config;
  config["types"] = "SSI";
  config["fields_to_keep"] = "all";

  string comma(",");
  string quote("\"");

  string string_field("Field 1");
  string quoted_comma("putting quotes around fields, allows commas");
  string number3(" 3");
  string s = string_field + comma + quote + quoted_comma + quote + comma + number3;

  string dummy("/usr/bar,,,,,,///,,,");

  {
    CSVParse csvparse;
    ASSERT_EQ(NO_ERR, csvparse.configure(config));

    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val(s);
    t->add_e()->set_s_val(dummy); // should not pass through YET
    t->set_version(0);

    csvparse.process_one(t);

    ASSERT_TRUE( t );

    ASSERT_EQ(3, t->e_size());
    ASSERT_EQ(string_field, t->e(0).s_val());
    ASSERT_EQ(quoted_comma, t->e(1).s_val());
    ASSERT_EQ(3, t->e(2).i_val());
  }

  {
    CSVParse csvp2;

    config["fields_to_keep"] = "1 2";
    config["types"] = "SSI";
    ASSERT_EQ(NO_ERR, csvp2.configure(config));

    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val(s);
    t->add_e()->set_s_val(dummy); // should not pass through YET
    t->set_version(0);
    csvp2.process_one(t);

    ASSERT_TRUE(t);

    ASSERT_EQ(2, t->e_size());

    ASSERT_EQ(quoted_comma, t->e(0).s_val());

    ASSERT_EQ(3, t->e(1).i_val());
  }
  
  
  {  //check for proper behavior on short tuples
    CSVParse csvp2;

    config["fields_to_keep"] = "1 2";
    config["types"] = "SSI";
    config["discard_off_size"] = "true";
    ASSERT_EQ(NO_ERR, csvp2.configure(config));

    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val("foo");   //too short
    t->set_version(0);
    csvp2.process_one(t);
    
    ASSERT_FALSE(t);

    
    t = boost::shared_ptr<Tuple>(new Tuple);
    t->add_e()->set_s_val("bar");
    t->add_e()->set_i_val(2); //too long
    t->add_e()->set_i_val(3);
    csvp2.process_one(t);

    ASSERT_FALSE( t );
  }
}

TEST(Operator, GrepOperator)
{
  map<string,string> config;
  config["pattern"] = "/usr";
  config["id"] = "1";

  StringGrep grepper;
  grepper.configure(config);

  {
    Tuple t;
    t.add_e()->set_s_val("foo");
    t.add_e()->set_s_val("/usr/bar"); //should match
    t.set_version(0);
    ASSERT_EQ(true, grepper.should_emit(t));
  }

  {
    Tuple t;
    t.add_e()->set_s_val("/user/foo");
    t.add_e()->set_s_val("/var/bar"); //should NOT match
    t.set_version(0);
    ASSERT_FALSE(grepper.should_emit(t));
  }

  {
    Tuple t;
    t.add_e()->set_s_val("foo");
    t.add_e()->set_s_val("/var/usr/bar"); //should match
    t.set_version(0);
    ASSERT_EQ(true, grepper.should_emit(t));
  }
}


TEST_F(COperatorTest, OperatorChain)
{
  boost::regex re1, re2;
  // For convenience, use the same config table for multiple operators
  map<string,string> config;

  // Create a chain of operators that reads lines from a file, filters them, and
  // stores the results
  shared_ptr<CFileRead> reader(new CFileRead);
  config["file"] = "src/tests/data/base_operators_data.txt";
  reader->configure(config);
  config.clear();

  shared_ptr<StringGrep> grepper1(new StringGrep);
  shared_ptr<StringGrep> grepper2(new StringGrep);
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  
  shared_ptr<OperatorChain> chain(new OperatorChain);
  reader->set_node(node.get());
  reader->add_chain(chain);
  chain->add_member(reader);
  chain->add_member(grepper1);
  chain->add_member(grepper2);
  chain->add_member(rec);


  config["pattern"] = "/usr";
  config["id"] = "0";
  re1.assign(config["pattern"]);
  grepper2->configure(config);
  // Filters comments, i.e. any line that has a '#' in it
  config["pattern"] = "^((?!#).)*$";
  re2.assign(config["pattern"]);
  grepper1->configure(config);
  chain->start();
  // Wait for reader to process entire file (alternatively, call stop() after a while)
  int i = 0;
  while (reader->isRunning() && i++ < 20) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(200));
  }
  

  ASSERT_GT(rec->tuples.size(), (size_t)4);
  // Each string should match both grep patterns, since they are in series
  boost::smatch matchResults;
  bool match;
  for (vector< shared_ptr<Tuple> >::iterator it = rec->tuples.begin(); it != rec->tuples.end(); it++) {
    match = boost::regex_search( (*it)->e(0).s_val(), matchResults, re1);
    match = (match && boost::regex_search((*it)->e(0).s_val(), matchResults, re2));
    ASSERT_TRUE(match);
  }
}

TEST(Operator,ParseOperator) {
  shared_ptr<xDummyReceiver> rec(new xDummyReceiver);
  GenericParse parse;
  parse.set_dest(rec);
  operator_config_t cfg;
  cfg["pattern"] = "(\\w+) (\\d+)";
  cfg["field_to_parse"] = "1";
  cfg["keep_unparsed"] = "True";
  cfg["types"] = "Si";
  ASSERT_EQ(NO_ERR, parse.configure(cfg));

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 1);
  extend_tuple(*t, "foo 7");
  extend_tuple(*t, 1.2);

  parse.process(t);
  ASSERT_EQ(1, parse.emitted_count());
  ASSERT_EQ((size_t)1, rec->tuples.size());
  boost::shared_ptr<Tuple> result = rec->tuples[0];
  ASSERT_EQ(4, result->e_size());
  ASSERT_EQ(1, result->e(0).i_val());
  ASSERT_EQ(string("foo"), result->e(1).s_val());
  ASSERT_EQ(7, result->e(2).i_val());
  ASSERT_EQ(1.2, result->e(3).d_val());

  //  cout << "finished valid test; no"

  GenericParse parse2;
  cfg["pattern"] = "(\\w+";
  cfg["field_to_parse"] = "0";
  cfg["types"] = "S";
  operator_err_t err = parse2.configure(cfg);
  ASSERT_GT(err.length(), 1U);

  // do almost exactly the first test again, but change "keep_unparsed" to
  // false, with correspondingly different asserts
  GenericParse parse3;
  shared_ptr<xDummyReceiver> rec3(new xDummyReceiver);
  parse3.set_dest(rec3);
  cfg["pattern"] = "(\\w+) (\\d+)";
  cfg["field_to_parse"] = "1";
  cfg["keep_unparsed"] = "False";
  cfg["types"] = "Si";
  ASSERT_EQ(NO_ERR, parse3.configure(cfg));

  boost::shared_ptr<Tuple> tt(new Tuple);
  extend_tuple(*tt, 1);
  extend_tuple(*tt, "foo 7");
  extend_tuple(*tt, 1.2);

  parse3.process(tt);
  ASSERT_EQ(1, parse3.emitted_count());
  ASSERT_EQ((size_t)1, rec3->tuples.size());
  result = rec3->tuples[0];
  ASSERT_EQ(2, result->e_size());
  ASSERT_EQ(string("foo"), result->e(0).s_val());
  ASSERT_EQ(7, result->e(1).i_val());
}
/* 

MOOTED BY REFACTORING

TEST(Operator, ExtendOperator) {

  ExtendOperator ex_1;
  shared_ptr<ExtendOperator> ex_host(new ExtendOperator);
  shared_ptr<DummyReceiver> rec(new DummyReceiver);

  operator_config_t cfg;
  cfg["types"] = "i";
  cfg["0"] = "1";
  ex_1.configure(cfg);
  ex_1.set_dest(ex_host);

  cfg["types"] = "s";
  cfg["0"] = "${HOSTNAME}";
  ex_host->configure(cfg);
  ex_host->set_dest(rec);

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);

  ex_1.process(t);

  ASSERT_EQ((size_t)1, rec->tuples.size());

  boost::shared_ptr<Tuple> result = rec->tuples[0];
  ASSERT_EQ(3, result->e_size());
  ASSERT_EQ(2, result->e(0).i_val()); //should preserve existing element[s]
  ASSERT_EQ(1, result->e(1).i_val()); //should preserve existing element[s]
  ASSERT_GT(result->e(2).s_val().length(), 2U);
  ASSERT_EQ( boost::asio::ip::host_name(), result->e(2).s_val());
  //  cout << "host name is "<< result->e(2).s_val() << endl;
  cout << "done" << endl;

}
*/

TEST(Operator, SampleOperator) {

  SampleOperator op;
  shared_ptr<xDummyReceiver> rec(new xDummyReceiver);
  operator_config_t cfg;
  cfg["fraction"] = "0.6";
  cfg["seed"] = "4";
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  op.set_dest(rec);

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);
  for (int i = 0; i < 1000; ++i) {
    op.process(t);
    t->mutable_e(0)->set_i_val(i);
  }
  ASSERT_GT((size_t)420, rec->tuples.size());
  ASSERT_LT((size_t)380, rec->tuples.size());

  cout << "done; " << rec->tuples.size() << " tuples received"<<endl;
}


TEST(Operator, HashSampleOperator) {
  int ROUNDS = 100, T_PER_ROUND = 100;
  HashSampleOperator op;
  shared_ptr<xDummyReceiver> rec(new xDummyReceiver);
  operator_config_t cfg;
  cfg["fraction"] = "0.5";
  cfg["hash_field"] = "0";
  cfg["hash_type"] = "I";

  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  op.set_dest(rec);

  int rounds_with_data = 0;
  for (int i=0; i < ROUNDS; ++i) {

    boost::shared_ptr<Tuple> t(new Tuple);
    extend_tuple(*t, i);

    int processed_before = rec->tuples.size();
    for (int j = 0; j < T_PER_ROUND; ++j) {
      op.process(t);
    }
    int processed_in_round = rec->tuples.size() - processed_before;
    if (processed_in_round > 0)
      rounds_with_data ++;
    ASSERT_EQ(0, processed_in_round % T_PER_ROUND); //all or none
  }
  cout << "done! " << rounds_with_data << " of " << ROUNDS << "values passed the hash" << endl;
  ASSERT_GT(  0.6 * ROUNDS, rounds_with_data);
  ASSERT_LT(  0.4 * ROUNDS, rounds_with_data);

}


TEST(Operator, TRoundingOperator) {
  TRoundingOperator op;

  operator_config_t cfg;
  cfg["fld_offset"] = "1";
  cfg["round_to"] = "5";
  cfg["in_type"] = "T";
  cfg["add_offset"] = "0";

  {
    operator_err_t err = op.configure(cfg);
    ASSERT_EQ(NO_ERR, err);

    cout << "starting operator" <<endl;
    op.start();

    shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
    extend_tuple(*t, "California");
    extend_tuple_time(*t, 6);
    op.process_one(t);

    ASSERT_EQ((time_t)5, t->e(1).t_val());
  }

  {
    // Test rounding a double and adding a constant;
    TRoundingOperator op2;
    cfg["in_type"] = "D";
    cfg["add_offset"] = "17";
    cfg["fld_offset"] = "2";

    ASSERT_EQ(NO_ERR, op2.configure(cfg));

    const double dExample_Micro_Epoch = 1231151151.510341;
    const time_t correct = (numeric_cast<time_t>(dExample_Micro_Epoch) / 5) * 5 + 17;

    shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
    extend_tuple(*t, "California");
    extend_tuple_time(*t, 6);
    extend_tuple(*t, dExample_Micro_Epoch);
    
    
    boost::shared_ptr<Tuple> result(new Tuple);
    result->CopyFrom(*t);

    op2.process_one(result);

    ASSERT_EQ((size_t)3, (size_t) result->e_size());
    ASSERT_EQ(correct, result->e(2).t_val()) << "input was " << fmt(*t) << endl
                                       << "result was " << fmt(*result) << endl;
  }
}

/*
//DISABLED_
TEST(Operator, DISABLED_UnixOperator) {
  // Doesn't work currently. popen creates half-duplex pipe we need full duplex 
  UnixOperator op;
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  operator_config_t cfg;
  cfg["cmd"] = "tee /tmp/tout";
  // cfg["cmd"] = "grep .";
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  op.set_dest(rec);
  op.start();

  shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
  extend_tuple(*t, "Bar");
  //  cout << "sending first tuple"<< endl;
  op.process(t);

  //  ASSERT_EQ((size_t)1, rec->tuples.size());

  extend_tuple(*t, "foo");
  op.process(t);
  cout << "sent tuple"<< endl;

  int tries = 0;
  size_t EXPECTED = 2;
  while (tries ++ < 10 && rec->tuples.size() < EXPECTED)
    js_usleep(50 * 1000);

  ASSERT_EQ(EXPECTED, rec->tuples.size());
}*/


TEST(Operator, URLToDomain) {
  URLToDomain op;
  operator_config_t cfg;
  cfg["field"] = "0";
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);

  shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
  extend_tuple(*t, "http://foo.com/blah");
  //  cout << "sending first tuple"<< endl;
  op.process_one(t);
  string dom = t->e(0).s_val();
  ASSERT_EQ(dom, "foo.com");
}

TEST(Operator, ExperimentTimeRewrite) {
  ExperimentTimeRewrite time_shift;
  
  operator_config_t cfg;
  cfg["field"] = "0";
  cfg["warp"] = "100";
  operator_err_t err = time_shift.configure(cfg);
  ASSERT_EQ(NO_ERR, err);

  time_t T_BASE = 1358640903; //jan 19, 2013
 
  shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
  extend_tuple_time(*t, T_BASE);
  
  time_shift.process_one(t);
  time_t shifted = t->e(0).t_val();
  time_t now = time(NULL);
  ASSERT_GT(shifted, now - 2);
  ASSERT_LT(shifted, now + 2);
  t->mutable_e(0)->set_t_val(T_BASE + 100);
  time_shift.process_one(t);
  time_t shifted_2 = t->e(0).t_val();
  ASSERT_EQ(1U, shifted_2 - shifted);
}
