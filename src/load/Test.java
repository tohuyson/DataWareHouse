package load;

import java.util.List;

public class Test {
	public static void main(String[] args) {
        try {
             
            String excelFilePath = "data\\dulieucanhan.xlsx";
             
            List<Student> listBooks = new ReadFromExcelFile().readBooksFromExcelFile(excelFilePath);
            StudentDAO studentDAO = new StudentDAO();
            studentDAO.insertListStudents(listBooks);
             
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
