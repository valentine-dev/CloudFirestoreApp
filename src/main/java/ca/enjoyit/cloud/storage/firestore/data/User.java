/**
 * 
 */
package ca.enjoyit.cloud.storage.firestore.data;

import com.google.cloud.Timestamp;

/**
 * @author Valentine Wu
 *
 */
public class User {
	private String id;
	private String first;
	private String middle;
	private String last;
	private int born;
	private String party;
	private Timestamp timestamp;

	public User() {
		// Must have a public no-argument constructor
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getMiddle() {
		return middle;
	}

	public void setMiddle(String middle) {
		this.middle = middle;
	}

	public String getLast() {
		return last;
	}

	public void setLast(String last) {
		this.last = last;
	}

	public int getBorn() {
		return born;
	}

	public void setBorn(int born) {
		this.born = born;
	}

	public String getParty() {
		return party;
	}

	public void setParty(String party) {
		this.party = party;
	}

	public String getTimestamp() {
		return timestamp.toString();
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = Timestamp.parseTimestamp(timestamp);
	}

}
