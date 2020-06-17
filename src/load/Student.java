package load;

public class Student {
	private String number;
	private String name;
	private String gender;
	private String identityCard;
	private String email;
	private String phone;
	private String address;
	
	public Student() {
	}
	
	public Student(String number, String name, String gender, String identityCard, String email, String phone,
			String address) {
		super();
		this.number = number;
		this.name = name;
		this.gender = gender;
		this.identityCard = identityCard;
		this.email = email;
		this.phone = phone;
		this.address = address;
	}
	
	public String getNumber() {
		return number;
	}

	public void setNumber(String number) {
		this.number = number;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getIdentityCard() {
		return identityCard;
	}
	public void setIdentityCard(String identityCard) {
		this.identityCard = identityCard;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}

}
