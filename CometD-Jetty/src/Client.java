/*
 * Copyright (c) 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.cometd.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;

/**
 *
 */
public class Client
{
    public static void main(String[] args) throws IOException
    {
        Client client = new Client();
        client.run();
    }

    private volatile String nickname = "";
    private volatile BayeuxClient client;
    private final ChatListener chatListener = new ChatListener();
    private final MembersListener membersListener = new MembersListener();

    private void run() throws IOException
    {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));

        String defaultURL = "http://107.21.229.172:8080/cometd-demo-2.4.3/cometd";
        System.err.printf("Enter Bayeux Server URL [%s]: ", defaultURL);

        String url = input.readLine();
        if (url == null)
            return;
        if (url.trim().length() == 0)
            url = defaultURL;

        while (nickname.trim().length() == 0)
        {
        	
            System.err.printf("Enter nickname: ");
            nickname = input.readLine();
            if (nickname == null)
                return;
        }

        client = new BayeuxClient(url, LongPollingTransport.create(null));
        client.getChannel(Channel.META_HANDSHAKE).addListener(new InitializerListener());
        client.getChannel(Channel.META_CONNECT).addListener(new ConnectionListener());

        client.handshake();
        boolean success = client.waitFor(1000, BayeuxClient.State.CONNECTED);
        if (!success)
        {
            System.err.printf("Could not handshake with server at %s%n", url);
            return;
        }

        for(int i=0 ; i<10; i++)
        {
            //String text = input.readLine();
        	StringBuffer sb = randomString(10,10);
        	try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	String text = sb.toString();
        	if (text == null || "\\q".equals(text))
            {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("user", nickname);
                data.put("membership", "leave");
                data.put("chat", nickname + " has left");
                client.getChannel("/chat/demo").publish(data);
                client.disconnect(1000);
                break;
            }

            Map<String, Object> data = new HashMap<String, Object>();
            data.put("user", nickname);
            data.put("chat", text);
            client.getChannel("/chat/demo").publish(data);
        }
        
        System.exit(0);
    }

    private void initialize()
    {
        client.batch(new Runnable()
        {
            public void run()
            {
                ClientSessionChannel chatChannel = client.getChannel("/chat/demo");
                chatChannel.unsubscribe(chatListener);
                chatChannel.subscribe(chatListener);

                ClientSessionChannel membersChannel = client.getChannel("/chat/members");
                membersChannel.unsubscribe(membersListener);
                membersChannel.subscribe(membersListener);

                Map<String, Object> data = new HashMap<String, Object>();
                data.put("user", nickname);
                data.put("membership", "join");
                data.put("chat", nickname + " has joined");
                chatChannel.publish(data);
            }
        });
    }

    private void connectionEstablished()
    {
        System.err.printf("system: Connection to Server Opened%n");
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("user", nickname);
        data.put("room", "/chat/demo");
        client.getChannel("/service/members").publish(data);
    }

    private void connectionClosed()
    {
        System.err.printf("system: Connection to Server Closed%n");
    }

    private void connectionBroken()
    {
        System.err.printf("system: Connection to Server Broken%n");
    }

    private class InitializerListener implements ClientSessionChannel.MessageListener
    {
        public void onMessage(ClientSessionChannel channel, Message message)
        {
            if (message.isSuccessful())
            {
                initialize();
            }
        }
    }

    private class ConnectionListener implements ClientSessionChannel.MessageListener
    {
        private boolean wasConnected;
        private boolean connected;

        public void onMessage(ClientSessionChannel channel, Message message)
        {
            if (client.isDisconnected())
            {
                connected = false;
                connectionClosed();
                return;
            }

            wasConnected = connected;
            connected = message.isSuccessful();
            if (!wasConnected && connected)
            {
                connectionEstablished();
            }
            else if (wasConnected && !connected)
            {
                connectionBroken();
            }
        }
    }
    

    private class ChatListener implements ClientSessionChannel.MessageListener
    {
        public void onMessage(ClientSessionChannel channel, Message message)
        {
            Map<String, Object> data = message.getDataAsMap();
            String fromUser = (String)data.get("user");
            String text = (String)data.get("chat");
            System.err.printf("%s: %s%n", fromUser, text);
        }
    }

    private class MembersListener implements ClientSessionChannel.MessageListener
    {
        public void onMessage(ClientSessionChannel channel, Message message)
        {
            Object data = message.getData();
            Object[] members = data instanceof List ? ((List)data).toArray() : (Object[])data;
            System.err.printf("Members: %s%n", Arrays.asList(members));
        }
    }
    
    private StringBuffer randomString(int number, int size){
	       //   response.setIntHeader("Refresh", 5);
        String variable = "";
        char[] chars = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','a','b','c','d','e','f','g','h','i','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

        StringBuffer ans_set = new StringBuffer(number * (size+1));
        
        while (number != 0){
         
      	  int string_length = size;
      	 
         
      	  for (char i=0; i<string_length; i++) {
      		  double rnum = Math.floor(Math.random() * chars.length);
      		  variable += chars[(int)rnum];
      	  	}
      	  ans_set.append(variable + ' ');
      	  number -= 1;
      	  variable = "";
        	}
		
        return ans_set; 
}
}
