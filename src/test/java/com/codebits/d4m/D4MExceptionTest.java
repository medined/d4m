package com.codebits.d4m;

import org.junit.Test;
import static org.junit.Assert.*;

/** Tests for the D4MException class.
 *
 * @author david
 */
public class D4MExceptionTest {
    
    private final static Throwable throwable = new Throwable();
        
    /** Test the constructor
     */
    @Test
    public void testConstructor() {
        assertNotNull(new D4MException());
    }
    
    /** Test the constructor with string
     */
    @Test
    public void testConstructor_with_string() {
        assertNotNull(new D4MException("message"));
    }
    
    /** Test the constructor with string and throwable
     */
    @Test
    public void testConstructor_with_string_and_throwable() {
        assertNotNull(new D4MException("message", throwable));
    }
    
    /** Test the constructor with throwable
     */
    @Test
    public void testConstructor_with_throwable() {
        assertNotNull(new D4MException(throwable));
    }
    
}
