package com.codebits.d4m;

/** Project-specific exception
 *
 * @author david medinets
 */
public class D4MException extends RuntimeException {

    /** Project-specific exception
     */
    public D4MException() {
    }

    /** Project-specific exception
     *
     * @param message message describing the exception.
     */
    public D4MException(String message) {
        super(message);
    }

    /** Project-specific exception
     *
     * @param message message describing the exception.
     * @param cause cause of the exception.
     */
    public D4MException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Project-specific exception
     *
     * @param cause cause of the exception.
     */
    public D4MException(Throwable cause) {
        super(cause);
    }

}
