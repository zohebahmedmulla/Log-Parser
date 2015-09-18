import negix
import unittest
import datetime


class MyTest(unittest.TestCase):
    # Test case for toat count og log entries
    # and for number of failures
    def test_count(self):
        negix.delete_table()
        log_file = open('C:\Users\zoheb\Desktop\project\logs.txt', 'r')
        self.assertEqual(negix.log_count(negix.process_log(log_file)),5)
        self.assertEqual(negix.Processing_failures(),1)
        
    # Test case for page view
    def test_page_view(self):
        negix.delete_table()
        log_file = open('C:\Users\zoheb\Desktop\project\logs.txt', 'r')
        d1=negix.page_count(negix.page_view(negix.process_log(log_file),'19/Jun/2012:09:17:27 +0100',50))
        d2={'Zyb.gif': 1, 'azb.gif': 1}
        a = {i:sorted(j) if isinstance(j, list) else j for i,j in d1.iteritems()}
        b = {i:sorted(j) if isinstance(j, list) else j for i,j in d2.iteritems()}
        self.assertDictEqual(a, b)
        
    # Test case for unique visit
    def test_unique_hit(self):
        negix.delete_table()
        log_file = open('C:\Users\zoheb\Desktop\project\logs.txt', 'r')
        d1=negix.page_count(negix.unique_hit(negix.process_log(log_file),'19/Jun/2012'))
        d2={'Zyb.gif': 1, 'azb.gif': 1, 'Yyb.gif': 1}
        a = {i:sorted(j) if isinstance(j, list) else j for i,j in d1.iteritems()}
        b = {i:sorted(j) if isinstance(j, list) else j for i,j in d2.iteritems()}
        self.assertDictEqual(a, b)

    # Test case for HTTPStatus count
    def test_httpstatus_count(self):
        negix.delete_table()
        log_file = open('C:\Users\zoheb\Desktop\project\logs.txt', 'r')
        http_code=200
        self.assertEqual(negix.httpstatus_count(negix.process_log(log_file),http_code),4)

    # Test case Total amount of time taken to process
    #extract and insert in to db
    # find unique hit and page view
    def test_time_taken(self):
        start=datetime.datetime.now()
        negix.delete_table()
        log_file = open('C:\Users\zoheb\Desktop\project\logs.txt', 'r')
        req=negix.process_log(log_file)
        negix.Processing_failures()
        negix.log_count(req)
        negix.page_count(negix.page_view(req,'19/Jun/2012:09:17:27 +0100',50))
        negix.page_count(negix.unique_hit(req,'19/Jun/2012'))
        end=datetime.datetime.now()
        print end-start
        
        
if __name__ == '__main__':
    unittest.main()
