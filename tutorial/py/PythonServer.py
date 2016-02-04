#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import glob
import sys
sys.path.append('../gen-py')
sys.path.insert(0, glob.glob('../../lib/py/build/lib*')[0])

from tutorial import Calculator
from tutorial.ttypes import InvalidOperation, Operation

from shared.ttypes import SharedStruct

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TJSONProtocol
from thrift.server.THttpServer import ResponseException

from six.moves import BaseHTTPServer

from thrift.server import TServer

# copied from thrift.server.THttpServer
class MyHttpServer(TServer.TServer):
  """A simple HTTP-based Thrift server

  This class is not very performant, but it is useful (for example) for
  acting as a mock version of an Apache-based PHP Thrift endpoint.
  """
  def __init__(self,
               processor,
               server_address,
               inputProtocolFactory,
               outputProtocolFactory=None,
               server_class=BaseHTTPServer.HTTPServer):
    """Set up protocol factories and HTTP server.

    See BaseHTTPServer for server_address.
    See TServer for protocol factories.
    """
    if outputProtocolFactory is None:
      outputProtocolFactory = inputProtocolFactory

    TServer.TServer.__init__(self, processor, None, None, None,
        inputProtocolFactory, outputProtocolFactory)

    thttpserver = self

    class RequestHander(BaseHTTPServer.BaseHTTPRequestHandler):
      def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("allow", "POST,OPTIONS")
        self.send_header("access-control-allow-origin", "*")
        self.send_header("access-control-allow-headers", "content-type")
        self.end_headers()

      def do_POST(self):
        # Don't care about the request path.
        itrans = TTransport.TFileObjectTransport(self.rfile)
        otrans = TTransport.TFileObjectTransport(self.wfile)
        itrans = TTransport.TBufferedTransport(
          itrans, int(self.headers['Content-Length']))
        otrans = TTransport.TMemoryBuffer()
        iprot = thttpserver.inputProtocolFactory.getProtocol(itrans)
        oprot = thttpserver.outputProtocolFactory.getProtocol(otrans)
        try:
          thttpserver.processor.process(iprot, oprot)
        except ResponseException as exn:
          exn.handler(self)
        else:
          self.send_response(200)
          self.send_header("content-type", "application/x-thrift")
          self.send_header("access-control-allow-origin", "*")
          self.send_header("access-control-allow-headers", "content-type")
          self.end_headers()
          self.wfile.write(otrans.getvalue())

    self.httpd = server_class(server_address, RequestHander)

  def serve(self):
    self.httpd.serve_forever()


class CalculatorHandler:
    def __init__(self):
        self.log = {}

    def ping(self):
        print('ping()')

    def add(self, n1, n2):
        print('add(%d,%d)' % (n1, n2))
        return n1 + n2

    def calculate(self, logid, work):
        print('calculate(%d, %r)' % (logid, work))

        if work.op == Operation.ADD:
            val = work.num1 + work.num2
        elif work.op == Operation.SUBTRACT:
            val = work.num1 - work.num2
        elif work.op == Operation.MULTIPLY:
            val = work.num1 * work.num2
        elif work.op == Operation.DIVIDE:
            if work.num2 == 0:
                x = InvalidOperation()
                x.whatOp = work.op
                x.why = 'Cannot divide by 0'
                raise x
            val = work.num1 / work.num2
        else:
            x = InvalidOperation()
            x.whatOp = work.op
            x.why = 'Invalid operation'
            raise x

        log = SharedStruct()
        log.key = logid
        log.value = '%d' % (val)
        self.log[logid] = log

        return val

    def getStruct(self, key):
        print('getStruct(%d)' % (key))
        return self.log[key]

    def zip(self):
        print('zip()')

if __name__ == '__main__':
    handler = CalculatorHandler()
    processor = Calculator.Processor(handler)
    pfactory = TJSONProtocol.TJSONProtocolFactory()

    server = MyHttpServer(processor, ("localhost", 9090), pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
