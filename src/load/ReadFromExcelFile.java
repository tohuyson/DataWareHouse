package load;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ReadFromExcelFile {
	public List<Student> readBooksFromExcelFile(String excelFilePath) throws IOException {
		List<Student> listBooks = new ArrayList<Student>();
		FileInputStream inputStream = new FileInputStream(new File(excelFilePath));

		Workbook workBook = getWorkbook(inputStream, excelFilePath);
		Sheet firstSheet = workBook.getSheetAt(0);
		Iterator<Row> rows = firstSheet.iterator();

		while (rows.hasNext()) {
			Row row = rows.next();
			Iterator<Cell> cells = row.cellIterator();
			Student student = new Student();

			while (cells.hasNext()) {
				Cell cell = cells.next();
				int columnIndex = cell.getColumnIndex();

				switch (columnIndex) {
				case 0:
					student.setNumber((String) getCellValue(cell));
					break;
				case 1:
					student.setName((String) getCellValue(cell));
					break;
				case 2:
					student.setGender((String) getCellValue(cell));
					break;
				case 3:
					student.setIdentityCard((String) getCellValue(cell));
					break;
				case 4:
					student.setEmail((String) getCellValue(cell));
					break;
				case 5:
					student.setPhone((String) getCellValue(cell));
					break;
				case 6:
					student.setAddress((String) getCellValue(cell));
					break;
				}
			}
			listBooks.add(student);
		}

		workBook.close();
		inputStream.close();

		return listBooks;
	}

	private Object getCellValue(Cell cell) {
		switch (cell.getCellType()) {
		case STRING:
			return cell.getStringCellValue();
	}

		return null;
	}

	private Workbook getWorkbook(FileInputStream inputStream, String excelFilePath) throws IOException {
		Workbook workbook = null;

		if (excelFilePath.endsWith("xlsx")) {
			workbook = new XSSFWorkbook(inputStream);
		} else if (excelFilePath.endsWith("xls")) {
			workbook = new HSSFWorkbook(inputStream);
		} else {
			throw new IllegalArgumentException("The specified file is not Excel file");
		}

		return workbook;
	}
}
