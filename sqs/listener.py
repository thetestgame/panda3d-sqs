"""
MIT License

Copyright (c) 2019 Nxt Games, LLC
Written by Jordan Maxwell 
07/30/2019

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from direct.directnotify.DirectNotifyGlobal import directNotify
from direct.showbase.DirectObject import DirectObject

from panda3d import core

import boto3
import boto3.session

class SQSListener(DirectObject):
    """
    Listens and processes incoming messages from an AWS SQS queue
    """

    notify = directNotify.newCategory('SQSListener')

    def __init__(self, queue_name, **kwargs):
        super().__init__()
        self.__queue_name = queue_name
        self.__poll_interval = kwargs.get('interval', core.ConfigVariableInt('sqs-default-poll', 30).value)
        self.__message_attribute_names = kwargs.get('message_attribute_names', [])
        self.__attribute_names = kwargs.get('attribute_names', [])
        self.__endpoint_url = kwargs.get('endpoint_url', None)
        self.__wait_time = kwargs.get('wait_time', core.ConfigVariableInt('sqs-default-wait-time', 0).value)
        self.__ssl = kwargs.get('ssl', True)
        self.__max_number_of_messages = kwargs.get('max_number_of_messages', core.ConfigVariableInt('sqs-default-max-msg', 1).value)
        self.__poll_task = None
        self.__session = boto3.session.Session()
        self.__region_name = kwargs.get('region_name', self.__session.region_name)
        self.__resource = None
        self.__queue = None
        
    @property
    def ready(self):
        """
        Valie is true when the listener is currently ready for use and has had its
        setup function called
        """

        return self.__resource != None and self.__queue != None

    @property
    def listening(self):
        """
        Value is true when the listener is currently listening to the sqs
        queue
        """

        return self.__poll_task != None

    def setup(self):
        """
        Performs setup operations on the listener object
        """

        if self.notify.getDebug():
            self.notify.debug('Performing setup for sqs listener: %s' % self.__class__.__name__)

        self.__initialize_client()

    def __initialize_client(self):
        """
        Performs boto3 client initialization steps on listener
        """

        # Create a new client if one does not exist
        if not self.__resource:
            self.__resource = self.__session.resource(
                service_name='sqs',
                region_name=self.__region_name,
                endpoint_url=self.__endpoint_url,
                use_ssl=self.__ssl)

        # Retrieve the queue instance from SQS
        if self.notify.getDebug():
            self.notify.debug('Retrieving queue: %s' % self.__queue_name)

        try:
            self.__queue = self.__resource.get_queue_by_name(QueueName=self.__queue_name)
        except Exception as e:
            self.notify.error('Failed to locate queue %s' % self.__queue_name)
            return

    def __poll_sqs_queue(self, task):
        """
        Performs polling operations on the sqs queue
        """

        if not self.ready:
            self.notify.warning('Failed to poll queue. Listener not ready')
            return task.done

        messages = self.__queue.receive_messages(
            MessageAttributeNames=self.__message_attribute_names,
            AttributeNames=self.__attribute_names,
            WaitTimeSeconds=self.__wait_time,
            MaxNumberOfMessages=self.__max_number_of_messages)

        for message in messages:
            # Handle the incoming message
            results = self.handle_message(
                message.body, 
                message.attributes, 
                message.message_attributes)

            # Check if we should delete the message from the queue
            if results:
                message.delete()

        return task.cont

    def start_listen(self):
        """
        Starts the listening operations if not started
        """

        if self.listening:
            self.notify.warning('Failed to start listen. Already listening')
            return

        self.__poll_interval = taskMgr.do_method_later(
            self.__poll_interval, 
            self.__poll_sqs_queue,
            '%s-sqs-listener' % self.__class__.__name__)

    def stop_listen(self):
        """
        Stops the listening operations if already started
        """

        if not self.listening:
            self.notify.warning('Failed to stop listen. Not currently listening')
            return

        taskMgr.remove(self.__poll_task)

    def destroy(self):
        """
        Performs destruction operations on the listener object
        """

        if self.notify.getDebug():
            self.notify.debug('Shutting down sqs listener: %s' % self.__class__.__name__)
        
        self.stop_listen() 

    def handle_message(self, body, attributes, message_attributes):
        """
        Handles incoming messages from the SQS queue. Returns true if the message was properly 
        handled and should be removed from the SQS queue.
        """

        if self.notify.getDebug():
            self.notify.debug('Received new message from %s' % self.__queue_name)

        return False
