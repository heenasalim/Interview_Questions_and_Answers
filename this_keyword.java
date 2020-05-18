
public class this_keyword {

	int slice = 16;
	
	this_keyword()
	{
	 this(3,4);
	
	 System.out.println("Calling parameterized constructor");
	
	}
	this_keyword(int x ,int y)
	{
		//this();
		
     System.out.println( x + y);
	}
	
	public static void printing(String  fruit)
	{
		int slice = 10;
		System.out.println(slice);
		
	}
	
	public static void  counting(String  fruit)
	{  
			
		int slice = 2;
		System.out.println(slice);
		//System.out.println(this.slice);
		
	}
	
	public  void  collecting(String  fruit)
	{  
		
		int slice = 2;
		System.out.println(slice);
		System.out.println(this.slice);
		
	}

	public static void main(String args[])
	{
		this_keyword s  = new this_keyword();
		
		printing("Mango");
		counting("Apple");
	
	}
	
	//this keyword
	//we cannot create local variable and instance variable of same name
	// so to access instance variable we can use this
	// we cannot use this in static method
	// it is also used to call one costructor from another constructor
	
	
}
