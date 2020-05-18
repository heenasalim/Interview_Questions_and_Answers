
public class Encapsulation_Parent_Class {

	
	//Encapsulation = Binding class members together
	// it is also known as data hiding
	// main purpose of the encapsulation is privacy
	// the methods are hidden using private keyword
	// other class can aceess only public methods of the class not private
	//using getter and setter methods
	// we can do correction in functions and it will reflect in all child class
	// user can only get data or update data
	// implemetation is purely hiddedn from user
	// example multiple users can get password and set password 
	//  users will not know how to create password
	// once we add correction to add OTP in createpassword method it will reflect
	// in all users class
	
	
	
	private int student_id;
	private String student_name;
	
	public void getstudentID()
	{
	System.out.println("for printing student_id" + student_id);
	}
	
	
   public void setstudentID (int x)
	{
	   student_id = x;
	   
	}
   }



   
	
