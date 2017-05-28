import java.util.Scanner;
import java.io.FileNotFoundException;
class ReadFiles {
	public void load_data(File file) {
		String[] data;
		Scanner line = new Scanner(new File(file));
		try {
			while(line.hasnextLine()) {
				data = line.nextLine().split("\t");
			}
		} catch (FileNotFoundException ex) {

		}
		
		System.out.println(Arrays.toString(data));

	}
}