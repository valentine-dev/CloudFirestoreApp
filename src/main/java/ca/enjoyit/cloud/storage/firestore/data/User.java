/**
 * 
 */
package ca.enjoyit.cloud.storage.firestore.data;

/**
 * @author Valentine Wu
 *
 */
public class User {
	private String id;
	private String first;
	private String middle;
	private String last;
	private Long born;
	
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
	public Long getBorn() {
		return born;
	}
	public void setBorn(Long born) {
		this.born = born;
	}
	

}
