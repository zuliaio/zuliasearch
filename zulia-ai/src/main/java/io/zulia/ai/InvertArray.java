import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;

public class InvertArray {
	public static void main(String[] args) {
		// Initialize NDManager (used for managing NDArray memory)
		NDManager manager = NDManager.newBaseManager();
		
		int[] data = { 1, 0, 1, 0, 1, 0, 1 };
		NDArray array = manager.create(data);
		
		// Invert the array: 1 -> 0 and 0 -> 1
		NDArray invertedArray = array.add(1).mul(-1).add(2);
		
		System.out.println("Original Array: " + array);
		System.out.println("Inverted Array: " + invertedArray);
	}
}