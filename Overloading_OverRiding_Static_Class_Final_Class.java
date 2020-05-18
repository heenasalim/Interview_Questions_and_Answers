
public class Overloading_OverRiding_Static_Class_Final_Class {
  
	
	int x = 20;
	static int b;
	Overloading_OverRiding_Static_Class_Final_Class()
	{	b++; }
	

	//constructor method which same name as class
    //it does not return anything
	
	//there are follwoing types of constructor
	//default constructor -
	//new keyword invokes default constructor to initialize object

	//if no constructor invoke default constructor get invoked
	
	//constructor with no parameter- 	//which is used to initialize class variables
	//parameterized constructor :- constructor which has parameters 
	
	
	//this keyword means current object
	// constructor can refer another constructor  using this keyword
	
	
	
	
	public static void print()
	{	 int s;
		System.out.println("printing no parameters");
	}
	
	public static void print(int x,int y)
	{
		b++;
		final int z = 9;
		print();   //static method accessing static method
		
		System.out.println("printing two parameter " + x + y);
	}
	public void print(int x, int y,int z )
	{     b++;		
		System.out.println("printing three parameter " + x + y + z );
	}
	
	public final void print(int x )
	{
		
		b++;
		final int  y = 4;
	    //	y = 2 ;  we cannot change final variable value
		System.out.println("printing one parameter "  + x);
	
	}
	
	//public abstract void abstract_method();
	
	   
	public static void main(String args[])
	{
		
		Overloading_OverRiding_Static_Class_Final_Class l = new Overloading_OverRiding_Static_Class_Final_Class();

		print();
		l.print(12);
		print(13,14);
	    l.print(13,14,15);
	
	}
}

final class Overriding extends Overloading_OverRiding_Static_Class_Final_Class {

/*	//we cannot override final method
 *  // we cannot change the value of final variable
 *  //  final class can extend any class but no class can extend final class
	
	public final void print(int x)  
	{	
	
	}
	
	*/

	// static variable :- the variable which belongs to class not object
	// so we can access its value without creating object
	// static variable initialized very first before other instance variables
	// we can access static variables in static as well as non-static methods
	
	//static method :-
	//we can override static method
	//so we can access static method without creating object
	// the method belongs to class not object
	// the static method can access only static variable
	// static method can access only static method
	// it cannot call non-sttaic method
	
	
	//static class - static class can access only static methods
	// we can create static class inside other class only
	
	public static void print(int x, int y )
	{
		
    //    print(12,13,14);  //non-static method
        print(11,13);      //accessing only static method
		System.out.println("printing three parameter " + x + y  );
	}
	
	
	public static class nestedclass  {
	
		
		 int x = 11;
		 
		 
		 
		public static void print(int x, int y )
		{
			
	    //    print(12,13,14);  //non-static method
	        print(11,13);   //static method
			System.out.println("printing three parameter again " + x + y  ); 	
		//System.out.println(super.x);}
	
			//cannot use super in staic class/methods
	}
	
	   public static void main(String args[])
	   {	print(4,5); 
	
	   
	   }
	}
	                                   }
	

 


 
 