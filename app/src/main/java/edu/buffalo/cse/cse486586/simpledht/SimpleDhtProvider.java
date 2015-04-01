package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.telephony.TelephonyManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.TreeSet;

public class SimpleDhtProvider extends ContentProvider {

	/* My port number (this device's port number) */
	static String myNodeId = null;
	static String TAG = null;

	Context context;
	static final int SERVER_PORT = 10000;
	static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};

	static final String URI_SCHEME = "content";
	static final String URI = "edu.buffalo.cse.cse486586.simpledht.provider";

	/* Important stuff */
	TreeSet<String> nodeInformation = new TreeSet<>();

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
				d. If 5554 is alive (TIMEOUT) - declare self as predecessor & successor
		 */

		context = getContext();

		/* Get own port ID */
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String myPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));
		myNodeId = genHash(myPortNumber);

		/* Tag to be used for all debug/error logs */
		TAG = "ANKIT" + myPortNumber;

		/* TODO: Get hashed ID */



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
	    try {
		    sha1 = MessageDigest.getInstance("SHA-1");
	    } catch (NoSuchAlgorithmException e) {
		    e.printStackTrace();
	    }
	    byte[] sha1Hash = sha1.digest(input.getBytes());
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
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contentOfFile;
	}

	/**
	 * buildUri() demonstrates how to build a URI for a ContentProvider.
	 *
	 * @param scheme
	 * @param authority
	 * @return the URI
	 */
	public static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
}
