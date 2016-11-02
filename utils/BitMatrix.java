package utils;

import java.util.BitSet;

/*
 * Implements a squared matrix containing bits.
 */
public class BitMatrix {
	
	
	private int size;
	private BitSet[] bitsets;
	
	
	public BitMatrix(int size) {
		
		this.size = size;
		
		this.bitsets = new BitSet[this.size];
        for (int i = 0; i < this.size; i++) {
            this.bitsets[i] = new BitSet(this.size);
        }
	}
	
	
	 /*
     * Sets the bit at a specified row and column position to true.
     * 
     * i - row position.
     * j - column position.
     */
    public void set(int i, int j) {
        this.bitsets[i].set(j);
    }
    
    
    /*
     * Sets the bit at a specified row and column position to false.
     * 
     * i - row position.
     * j - column position.
     */
    public void clear(int i, int j) {
        this.bitsets[i].clear(j);
    }



    /*
     * Returns the value of the bit at a specific position in the matrix.
     * 
     * i - The row of the bit.
     * j - The column of the bit.
     * 
     */
    public boolean get(int i, int j) {
        return this.bitsets[i].get(j);
    }
}
