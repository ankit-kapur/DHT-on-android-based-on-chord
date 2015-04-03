package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
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
	static final String URI_AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";

	/* Important stuff */
	TreeMap<String, Integer> nodeInformation = new TreeMap<>();
	static int predecessorId = 0, successorId = 0;
	static boolean is5554Alive = false;
	static boolean isQueryAnswered = false;
	static String resultOfMyQuery = null;

	List<String> keysInserted = new ArrayList<>();

	@Override
	public boolean onCreate() {

		/* 	1. Get own port ID
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

		/* Declare self as predecessor and successor */
		predecessorId = myPortNumber;
		successorId = myPortNumber;

		/* Tag - to be used for all debug/error logs */
		TAG = "aANKIT" + myPortNumber;

		if (myPortNumber == 5554) {

			/* Put self in the nodeInformation map */
			nodeInformation.put(myHashedId, myPortNumber);
		} else {
			/* Send a join request to 5554 (11108) */
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SEND_JOIN_REQUEST));
		}

		/* Create a server socket and a thread (AsyncTask) that listens on the server port */
		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			Log.getStackTraceString(e);
			return false;
		}

		return false;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
	    /* Only need to use the first two parameters, uri & selection */

		if (selection.equals("\"*\"")) {
		    /* TODO: If “*” is given as the selection parameter to delete(),
		   then you need to delete all <key, value> pairs stored in your entire DHT. */

		} else if (selection.equals("\"@\"")) {
		    /* Delete all files on the local partition */

			for (String key : keysInserted)
				context.deleteFile(key);
			keysInserted.clear();

		} else {
			/* Normal case. 'selection' is the key */

			/* Does it belong here? */
			String hashedKey = genHash(selection);
			boolean belongsHere = doesItBelongHere(hashedKey);

			if (belongsHere) {
				/* --- The key is on THIS partition */

				/* TODO: Check if this works */
				context.deleteFile(selection);
				keysInserted.remove(selection);
			} else {
				/* TODO: The key is NOT on this partition. Send to successor */
			}
		}

		return 0;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
	                    String sortOrder) {

		/* Make the cursor */
		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
		Log.d(TAG, "[Query] " + "Query received. Selection ==> " + selection);

		if (selection.equals("\"*\"")) {
			/* TODO: Handle case for "*" */

			/* The value belongs somewhere else. Ask the successor if it has it */
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_STAR_REQUEST), String.valueOf(myPortNumber), " ");

			/* Wait until the query is answered */
			isQueryAnswered = false;
			while (!isQueryAnswered);

			/* The "*" query has been answered by all. Store the results in the cursor and return them. */
			/* Split up the key-value pairs in "resultOfMyQuery" */
			if (resultOfMyQuery.contains(",")) {
				String[] starResults = resultOfMyQuery.trim().split(",");
				for (String starResult : starResults) {
					/* starResult is made of key===value */
					String[] keyValue = starResult.split("===");

					/* Add row to the cursor with the key & value */
					String[] columnValues = {keyValue[0], keyValue[1]};
					matrixCursor.addRow(columnValues);
				}
			}

			Log.v(TAG, "Query for '*' complete. No. of rows retrieved ==> " + matrixCursor.getCount());
		} else if (selection.equals("\"@\"")) {
		    /* TODO: Handle case for "@" */

			/* Return all key-value pairs on this local partition */
			for (String key : keysInserted) {
				addRowToCursor(key, matrixCursor);
			}

			Log.v(TAG, "Query for '@' complete. No. of rows retrieved ==> " + matrixCursor.getCount());
		} else {
			/* ---- Normal key ("selection" is the key) */
			String hashedKey = genHash(selection);

			/* Does it belong here? */
			boolean belongsHere = doesItBelongHere(hashedKey);

			if (belongsHere) {
				/* The key-value pair is here. Read it into the cursor */
				Log.d(TAG, "[Query] " + selection + " ==> belongs here. Reading.");
				addRowToCursor(selection, matrixCursor);
			} else {
				/* The value belongs somewhere else. Ask the successor if it has it */
				Log.d(TAG, "[Query] " + selection + " ==> does not belong here. Asking successor => " + successorId);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_FIND_REQUEST), String.valueOf(myPortNumber), selection);

				/* Wait until the query is answered */
				isQueryAnswered = false;
				while (!isQueryAnswered);

				/* Query has been answered. Store the results in cursor and return them. */
				String[] columnValues = {selection, resultOfMyQuery};
				matrixCursor.addRow(columnValues);
			}

			Log.v(TAG, "[Query] No. of rows retrieved ==> " + matrixCursor.getCount());
		}

		return matrixCursor;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String msgKey = (String) values.get("key");
		String msgValue = (String) values.get("value");
		String hashedKey = genHash(msgKey);

		/* Does it belong here? */
		boolean belongsHere = doesItBelongHere(hashedKey);

		if (belongsHere) {
			/* Since it belongs here, write the content values to internal storage */
			Log.d(TAG, "[Insert] " + msgKey + " belongs here. Inserting.");
			writeToInternalStorage(msgKey, msgValue);
			keysInserted.add(msgKey);
		} else {
			/* Doesn't belong here. Pass it on until it reaches the right place. */
			Log.d(TAG, "[Insert] " + msgKey + " ==> does not belong here. Passing to successor => " + successorId);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.INSERT_REQUEST), msgKey, msgValue);

			/* TODO: Check if this works */
		}

		return null;
	}

	private boolean doesItBelongHere(String msgKeyHashed) {
	    /*
	        Cases:
	          1. if I am my own successor and predecessor ---> it belongs HERE
	          2. if I'm the 1st in the ring
	                a. if msg-key > predecessor OR msg-key < my-key ---> it belongs HERE
	          3. else if msg-key is between my predecessor and me ---> it belongs HERE
	     */

		boolean belongsHere = false;
		String predecessorHashedId = genHash(predecessorId);
		String successorHashedId = genHash(successorId);

		if (myHashedId.equals(predecessorHashedId) && myHashedId.equals(successorHashedId))
			belongsHere = true;
		else if (myHashedId.compareTo(predecessorHashedId) < 0) {
			/* I'm the 1st in the ring */
			if (msgKeyHashed.compareTo(predecessorHashedId) > 0 || msgKeyHashed.compareTo(myHashedId) < 0)
				/* The message key is larger than the largest key, OR smaller than the smallest */
				belongsHere = true;
		} else if (msgKeyHashed.compareTo(predecessorHashedId) > 0 && msgKeyHashed.compareTo(myHashedId) <= 0)
			/* Normal case. The key is between my predecessor and me */
			belongsHere = true;

		Log.d(TAG, "[belongsHere = " + belongsHere + "] hashedKey = " + msgKeyHashed + ", predecessor = " + predecessorId + "(" + predecessorHashedId + "), successor = " + successorId + "(" + successorHashedId + ")");
		return belongsHere;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	private String genHash(int input) {
		return genHash(String.valueOf(input));
	}

	private String genHash(String input) {
		MessageDigest sha1;
		Formatter formatter = new Formatter();
		byte[] sha1Hash;

		try {
			sha1 = MessageDigest.getInstance("SHA-1");
			sha1Hash = sha1.digest(input.getBytes());
			for (byte b : sha1Hash) {
				formatter.format("%02x", b);
			}
		} catch (Exception e) {
			Log.e("ERROR " + TAG, Log.getStackTraceString(e));
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
					Log.d(TAG, "Waiting to accept socket");
					clientSocket = serverSocket.accept();
					Log.d(TAG, "Accepted socket");

					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String incomingString = bufferedReader.readLine();

					Log.d(TAG, "incoming ==> " + incomingString);
					String incoming[] = incomingString.split("##");
					Mode mode = Mode.valueOf(incoming[0]);

					switch (mode) {
						case JOIN_REQUEST:
							/* --- Got a join request */

							/* Update the nodeInformation list */
							String sendersActualID = incoming[1];
							nodeInformation.put(genHash(sendersActualID), Integer.parseInt(sendersActualID));

							/* Respond to EVERYONE, informing them of their predecessor and successor. */
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SEND_JOIN_RESPONSE));
							break;

						case JOIN_RESPONSE:
							/* --- Got a response from 5554 for the join request I (OR SOMEONE ELSE) had sent */
							is5554Alive = true;
							Log.d(TAG, "Got neighbour information from 5554 ==> " + incomingString);

							String IDs = incoming[2];
							String[] parts = IDs.split("%%");
							predecessorId = Integer.parseInt(parts[0]);
							successorId = Integer.parseInt(parts[1]);

							break;

						case INSERT_REQUEST:
							/* A request for insertion received.
							   Message structure ===> <mode> ## <key> ## <value> */

							String msgKey = incoming[1];
							String msgValue = incoming[2];

							/* Make URI and ContentValues object for the insert method call */
							ContentValues contentValues = new ContentValues();
							contentValues.put("key", msgKey);
							contentValues.put("value", msgValue);
							Uri uri = buildUri(URI_SCHEME, URI_AUTHORITY);

							/* Call insert */
							insert(uri, contentValues);

							break;

						case QUERY_FIND_REQUEST:
							String originator = incoming[1];
							String keyToFind = incoming[2];

							if (doesItBelongHere(genHash(keyToFind))) {
								/* The key is present here. Find it and inform the originator */
								String queryResult = readFromInternalStorage(keyToFind);
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_RESULT_FOUND), originator, queryResult);
							} else {
								/* Not here. Pass on to successor */
								Log.d(TAG, "[Query] " + keyToFind + " ==> does not belong here. Passing on to successor => " + successorId);
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_FIND_REQUEST), originator, keyToFind);
							}
							break;

						case QUERY_RESULT_FOUND:
							resultOfMyQuery = incoming[1];
							isQueryAnswered = true;
							break;

						case QUERY_STAR_REQUEST:
							originator = incoming[1];
							String aggregatedResult = incoming[2];

							/* Append all your key-value pairs to the result */
							for (String key : keysInserted) {
								String value = readFromInternalStorage(key);
								aggregatedResult += key + "===" + value + ",";
							}

							if (originator.equals(String.valueOf(myPortNumber))) {
								/* I am the originator. Stop here. */

								/* Remove trailing comma */
								if (!aggregatedResult.isEmpty() && aggregatedResult.charAt(aggregatedResult.length()-1) == ',')
									aggregatedResult = aggregatedResult.substring(0, aggregatedResult.length()-1);

								Log.d(TAG, "All '*' results received ==> " + aggregatedResult);
								resultOfMyQuery = aggregatedResult;
								isQueryAnswered = true;
							} else {
								/* TODO: Forward to successor */
								Log.d(TAG, "Appended my stuff to the '*' query ==> " + aggregatedResult);
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_STAR_REQUEST), originator, aggregatedResult);
							}

							break;
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
			String messageToBeSent = null;
			int destinationPortId;

			switch (mode) {

				case SEND_JOIN_REQUEST:
					/* Send join request to 5554 */
					destinationPortId = 5554 * 2;
					messageToBeSent = Mode.JOIN_REQUEST.toString() + "##" + myPortNumber + "##" + "null";
					Log.d(TAG, "Sending JOIN request to 5554 ==> " + messageToBeSent);

					sendOnSocket(messageToBeSent, destinationPortId);

					break;

				case SEND_JOIN_RESPONSE:
					/* Tell everyone about their neighbours */
					for (String hashedPortId : nodeInformation.keySet()) {

						/* Which port are we sending the message on */
						int portId = nodeInformation.get(hashedPortId);
						destinationPortId = portId * 2;

						/* Make the message to be sent */
						String sendMode = Mode.JOIN_RESPONSE.toString();
						messageToBeSent = sendMode + "##" + myPortNumber + "##" + getPredecessorAndSuccessor(hashedPortId);

						Log.d(TAG, "Sending neighbour information to " + portId + " ==> " + messageToBeSent);

						/* Send the message */
						sendOnSocket(messageToBeSent, destinationPortId);
					}

					break;

				case INSERT_REQUEST:
					/* Construct message as ===> <mode> ## <key> ## <value> */
					String msgKey = (String) params[1];
					String msgValue = (String) params[2];
					messageToBeSent = Mode.INSERT_REQUEST.toString() + "##" + msgKey + "##" + msgValue;

					sendOnSocket(messageToBeSent, successorId * 2);
					break;

				case QUERY_FIND_REQUEST:
					String originator = (String) params[1];
					String keyToFind = (String) params[2];
					messageToBeSent = Mode.QUERY_FIND_REQUEST.toString() + "##" + originator + "##" + keyToFind;

					sendOnSocket(messageToBeSent, successorId * 2);
					break;

				case QUERY_RESULT_FOUND:
					/* The key in the query was found here. Give it's value to the originator */
					originator = (String) params[1];
					String queryResult = (String) params[2];
					messageToBeSent = Mode.QUERY_RESULT_FOUND.toString() + "##" + queryResult;

					sendOnSocket(messageToBeSent, Integer.parseInt(originator) * 2);
					break;

				case QUERY_STAR_REQUEST:
					/* Ask successor to add on ALL his key-value pairs and pass the joint */
					originator = (String) params[1];
					String aggregatedResult = (String) params[2];
					messageToBeSent = Mode.QUERY_STAR_REQUEST.toString() + "##" + originator + "##" + aggregatedResult;

					Log.d(TAG, "Forwarding '*' query to successor: " + successorId * 2);
					sendOnSocket(messageToBeSent, successorId * 2);
					break;
			}

			return null;
		}

		/* Send to successor */
		private void sendOnSocket(String messageToBeSent, int destinationPort) {
			Socket sendSocket = null;
			try {
				sendSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						destinationPort);
				PrintWriter printWriter = new PrintWriter(sendSocket.getOutputStream(), true);
				printWriter.println(messageToBeSent);
			} catch (IOException e) {
				Log.e("ERROR " + TAG, Log.getStackTraceString(e));
			} finally {
				if (sendSocket != null) {
					try {
						sendSocket.close();
					} catch (IOException e) {
						Log.e("ERROR " + TAG, Log.getStackTraceString(e));
					}
				}
			}
		}
	}


	/* ---------- UTIL methods and enums */

	private String getPredecessorAndSuccessor(String hashedPortId) {
		/* Return ==> predecessorID %% successorID */
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

	private void addRowToCursor(String key, MatrixCursor matrixCursor) {
		String fileContent = readFromInternalStorage(key);
		if (fileContent != null && fileContent.length() > 0) {
			String[] columnValues = new String[2];
			columnValues[0] = key;
			columnValues[1] = fileContent;
			matrixCursor.addRow(columnValues);
		}
	}

	public enum Mode {
		JOIN_REQUEST, JOIN_RESPONSE, SEND_JOIN_RESPONSE, SEND_JOIN_REQUEST, INSERT_REQUEST, QUERY_FIND_REQUEST, QUERY_RESULT_FOUND, QUERY_STAR_REQUEST
	}
}
