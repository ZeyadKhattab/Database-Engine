package Main;

public class DBAppException extends Exception {
	DBAppException(String errorMessage){
		super(errorMessage);
		
	}
	DBAppException(){
		super();
	}

}
