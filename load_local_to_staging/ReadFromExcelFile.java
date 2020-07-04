package load_local_to_staging;

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
	public List<Student> readStudentsFromExcelFile(String excelFilePath) throws IOException {
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
					student.setStt((String) String.valueOf(getCellValue(cell)));
					break;
				case 1:
					student.setMasv((String) String.valueOf(getCellValue(cell)));
					break;
				case 2:
					student.setHolot((String) getCellValue(cell));
					break;
				case 3:
					student.setTen((String) getCellValue(cell));
					break;
				case 4:
					student.setNgaysinh((String) String.valueOf(getCellValue(cell)));
					break;
				case 5:
					student.setMalop((String) getCellValue(cell));
					break;
				case 6:
					student.setTenlop((String) getCellValue(cell));
					break;

				case 7:
					student.setDthoai((String) String.valueOf(getCellValue(cell)));
					break;
				case 8:
					student.setEmail((String) getCellValue(cell));
					break;
				case 9:
					student.setQuequan((String) getCellValue(cell));
					break;
				case 10:
					student.setGhichu((String) getCellValue(cell));
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
		case NUMERIC:
			return (int) cell.getNumericCellValue();
//		default:
//			cell.getStringCellValue();
		}
		return cell.getStringCellValue();
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
