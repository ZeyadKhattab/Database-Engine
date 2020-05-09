package NameInsertedBefore;

public class DBAppException extends Exception {
	DBAppException(String errorMessage){
		super(errorMessage);
		
	}
	DBAppException(){
		super();
	}

}
