package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

public class SimpleDhtProvider extends ContentProvider {

	/* My port number (this device's port number) */
	int myPortNumber = 0;
	static String myHashedId = null;
	static String TAG = null;

	Context context;
	static final int SERVER_PORT = 10000;
	//static final String[] PORT_ID_LIST = {"11108", "11112", "11116", "11120", "11124"};

	static final String URI_SCHEME = "content";
	static final String URI = "edu.buffalo.cse.cse486586.simpledht.provider";

	/* Important stuff */
	TreeMap<String, Integer> nodeInformation = new TreeMap<>();
	int predecessorId = 0, successorId = 0;
	boolean is5554Alive = false;

	@Override
	public boolean onCreate() {

		/* TODO:
			1. Get own port ID
			2. If I am 5554
				a. Declare self as predecessor and successor
				a. Create a parallel AsyncTask that:
					i. Listens for join requests
					ii. If a join request is received:
						1. Update the nodeInformation list
						2. Respond to EVERYONE, informing them of their predecessor and successor.
			3. If I'm NOT 5554
				a. Send a join request to 5554 (11108)
				b. Wait for a response
					c. If 5554 is alive - receive predecessor & successor (and set them)
					d. If 5554 is NOT alive (TIMEOUT) - declare self as predecessor & successor
		 */

		context = getContext();

		/* Get own port ID */
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPortNumber = Integer.parseInt(portStr);

		/* Get hashed ID */
		myHashedId = genHash(String.valueOf(myPortNumber));

		/* Tag - to be used for all debug/error logs */
		TAG = "ANKIT" + myPortNumber;

		if (myPortNumber == 5554) {
			/* Declare self as predecessor and successor */
			predecessorId = myPortNumber;
			successorId = myPortNumber;

			/* Put self in the nodeInformation map */
			nodeInformation.put(myHashedId, myPortNumber);

			try {
			    /* Create a server socket and a thread (AsyncTask) that listens on the server port */
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				Log.e(TAG, "Can't create a ServerSocket");
				Log.getStackTraceString(e);
				return false;
			}


		} else {
			/* TODO: Send a join request to 5554 (11108) */
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SEND_JOIN_REQUEST));

		}

		return false;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
	    /* Only need to use the first two parameters, uri & selection */


		if (selection.equals("\\”*\\””")) {
		    /* TODO: If “*” is given as the selection parameter to delete(),
	       then you need to delete all <key, value> pairs stored in your entire DHT. */

		} else if (selection.equals("\\”@\\””")) {
	        /* TODO: Handle case for "@" */
		} else {
			/* TODO: Normal key */
			String key = selection;
		}

		return 0;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
	                    String sortOrder) {


		if (selection.equals("\\”*\\””")) {
			/* TODO: Handle case for "*" */
		} else if (selection.equals("\\”@\\””")) {
	        /* TODO: Handle case for "@" */
		} else {
			/* TODO: Normal key */
			String key = selection;
		}

		return null;
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {

	    /*
	        TODO:
	          1. if key <= me
	                a. if key is > predecessor
	                    i. if my-key is < successor --> it belongs here
	                    ii. else --> send to successor
	                b. else --> send to successor
	          2. else if key > me
	                a. if my-key is < predecessor AND predecessor < msg-key --> it belongs here
	                b. else --> send to successor
	     */

		String msgKey = (String) values.get("key");
		String msgValue = (String) values.get("value");

		String key = genHash(msgKey);


	    /* TODO: Write only if this key-value pair belongs here, otherwise send it to...successor? */
		// writeToInternalStorage

		return null;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	private String genHash(String input) {
		MessageDigest sha1 = null;
		byte[] sha1Hash = null;
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
			sha1Hash = sha1.digest(input.getBytes());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private void writeToInternalStorage(String fileName, String contentOfFile) {
		try {
			FileOutputStream stream = context.openFileOutput(fileName, Context.MODE_WORLD_WRITEABLE);
			stream.write(contentOfFile.getBytes());
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String readFromInternalStorage(String fileName) {
		String contentOfFile = "";
		try {
			File file = context.getFileStreamPath(fileName);
			if (file.exists()) {
				FileInputStream stream = context.openFileInput(fileName);
				int byteContent;
				if (stream != null) {
					while ((byteContent = stream.read()) != -1)
						contentOfFile += (char) byteContent;
					stream.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contentOfFile;
	}

	/* buildUri() demonstrates how to build a URI for a ContentProvider. */
	public static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	/* ServerTask is an AsyncTask that should handle incoming messages. */
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

            /* Server code that receives messages and passes them to onProgressUpdate(). */
			Socket clientSocket;

			try {
				while (true) {
					clientSocket = serverSocket.accept();

					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String incomingString = bufferedReader.readLine();

					String incoming[] = incomingString.split("$$");
					Mode mode = Mode.valueOf(incoming[0]);
					String sendersActualID = incoming[1];

					switch (mode) {
						case JOIN_REQUEST:
							/* --- Got a join request */

							/* Update the nodeInformation list */
							nodeInformation.put(genHash(sendersActualID), Integer.parseInt(sendersActualID));

							/* Respond to EVERYONE, informing them of their predecessor and successor. */
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SEND_JOIN_RESPONSE));
							break;

						case JOIN_RESPONSE:
							/* --- Got a response from 5554 for the join request I had sent */
							is5554Alive = true;

							/* TODO ================================================== */
							
					}

				}
			} catch (IOException e) {
				Log.e("ERROR " + TAG, Log.getStackTraceString(e));
			}

			return null;
		}


	}

	private class ClientTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... params) {

			String modeString = (String) params[0];
			Mode mode = Mode.valueOf(modeString);
			Socket sendSocket = null;

			switch (mode) {

				case SEND_JOIN_REQUEST:
					/* Send join request */
					try {
						try {
							int destinationPortId = 5554 * 2;
							String messageToBeSent = Mode.JOIN_REQUEST.toString() + "$$" + myPortNumber + "$$" + "null";

							sendSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									destinationPortId);
							PrintWriter printWriter = new PrintWriter(sendSocket.getOutputStream(), true);
							printWriter.println(messageToBeSent);

							/* Wait, then check if 5554 replied */
							Timer timer = new Timer();
							timer.schedule(new TimerTask() {
								public void run() {
									if (!is5554Alive) {
										/* 5554 is DEAD. Declare self as predecessor and successor */
										predecessorId = myPortNumber;
										successorId = myPortNumber;
									}
								}
							}, 3000);
						} finally {
							if (sendSocket != null)
								sendSocket.close();
						}
					} catch (IOException e) {
						Log.e("ERROR " + TAG, Log.getStackTraceString(e));
					}
					break;

				case SEND_JOIN_RESPONSE:
					try {

						for (String hashedPortId : nodeInformation.keySet()) {

							/* Which port are we sending the message on */
							int portId = nodeInformation.get(hashedPortId);
							String destinationPortId = String.valueOf(portId * 2);

							/* Make the message to be sent ==> predecessorID %% successorID */
							String sendMode = Mode.JOIN_RESPONSE.toString();
							String messageToBeSent = sendMode + "$$" + myPortNumber + "$$" + getPredecessorAndSuccessor(hashedPortId);

							/* Send the message */
							try {
								sendSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(destinationPortId));
								PrintWriter printWriter = new PrintWriter(sendSocket.getOutputStream(), true);
								printWriter.println(messageToBeSent);
							} finally {
								if (sendSocket != null)
									sendSocket.close();
							}
						}

					} catch (IOException e) {
						Log.e("ERROR " + TAG, Log.getStackTraceString(e));
					}

					break;
			}

			return null;
		}
	}


	/* ---------- UTIL methods and enums */

	private String getPredecessorAndSuccessor(String hashedPortId) {
		String predecessorAndSuccessorHashedIDs;
		String predecessorID = null, successorId;
		Iterator<String> iterator = nodeInformation.keySet().iterator();
		while (iterator.hasNext()) {
			String currentKey = iterator.next();
			if (currentKey.equals(hashedPortId))
				break;
			predecessorID = String.valueOf(nodeInformation.get(currentKey));
		}

		/* Get predecessor */
		if (predecessorID == null)
			predecessorID = String.valueOf(nodeInformation.get(nodeInformation.lastKey()));

		/* Get successor */
		if (iterator.hasNext())
			successorId = String.valueOf(nodeInformation.get(iterator.next()));
		else
			successorId = String.valueOf(nodeInformation.get(nodeInformation.firstKey()));

		/* Construct the return string */
		predecessorAndSuccessorHashedIDs = predecessorID + "%%" + successorId;

		return predecessorAndSuccessorHashedIDs;
	}

	public enum Mode {
		JOIN_REQUEST, JOIN_RESPONSE, SEND_JOIN_RESPONSE, SEND_JOIN_REQUEST
	}
}
