package com.bah.na.asc.storage.record.management.elasticsearch;

/**
 * RecordMetadata holds the meta information for each record that is about to
 * expire.
 * 
 * @author Darren Henderson
 * @version %I%, %G%
 * @since 1.0
 */
public class RecordMetadata{
	private String id;
	private String email;
	private String expires;

	/**
	 * Construct an instance of this class.
	 * 
	 * @since 1.0
	 */
	public RecordMetadata(){

	}

	/**
	 * Construct an instance of this class. recordName is the name of a record
	 * and contains all of the meta information formatted in the following way:
	 * "1234-darren@birch.asc.bah.com-2016-12-02". This constructor will extract
	 * the meta information from recordName and store each value in a member
	 * variable
	 * 
	 * @param recordName
	 *            the name of a record which contains all of the meta
	 *            information formatted in a specific way
	 * @since 1.0
	 */
	public RecordMetadata(String recordName){
		// parse out the unique ID, e-mail address, and expiration date from the
		// record name
		// sample record name: "1234-darren@birch.asc.bah.com-2016-12-02"
		int length = recordName.length();

		// extract the unique ID
		StringBuffer uniqueID = new StringBuffer();

		int iRecordName = 0;
		boolean foundDash = false;
		// keep adding characters from the record name to uniqueID until we run
		// into the dash
		while(!foundDash && iRecordName < length){
			char ch = recordName.charAt(iRecordName);
			if(ch != '-'){
				uniqueID.append(ch);
			}else{
				foundDash = true;
			}

			iRecordName++;
		}

		setId(uniqueID.toString());

		// extract the email address
		StringBuffer email = new StringBuffer();

		foundDash = false;
		// keep adding characters from the record name to email until we run
		// into the dash
		while(!foundDash && iRecordName < length){
			char ch = recordName.charAt(iRecordName);
			if(ch != '-'){
				email.append(ch);
			}else{
				foundDash = true;
			}

			iRecordName++;
		}

		setEmail(email.toString());

		// extract the expiration date
		StringBuffer expirationDate = new StringBuffer();

		foundDash = false;
		// keep adding characters from the record name to expirationDate until
		// we get to the end of the string
		while(iRecordName < length){
			char ch = recordName.charAt(iRecordName);
			expirationDate.append(ch);

			iRecordName++;
		}

		setExpires(expirationDate.toString());
	}

	/**
	 * Specify the unique identifier field of the record.
	 * 
	 * @param id
	 *            the unique identifier field of the record
	 * @since 1.0
	 */
	public void setId(String id){
		this.id = id;
	}

	/**
	 * Specify the e-mail address field of the record.
	 * 
	 * @param email
	 *            the e-mail address field of the record
	 * @since 1.0
	 */
	public void setEmail(String email){
		this.email = email;
	}

	/**
	 * Specify the expiration date field of the record.
	 * 
	 * @param expires
	 *            the expiration date field of the record
	 * @since 1.0
	 */
	public void setExpires(String expires){
		this.expires = expires;
	}

	/**
	 * Return the unique identifier field of the record.
	 * 
	 * @since 1.0
	 */
	public String getId(){
		return id;
	}

	/**
	 * Return the e-mail address field of the record.
	 * 
	 * @since 1.0
	 */
	public String getEmail(){
		return email;
	}

	/**
	 * Return the expiration date field of the record.
	 * 
	 * @since 1.0
	 */
	public String getExpires(){
		return expires;
	}
}
