package com.dydabo.blackbox;

import org.junit.jupiter.api.Test;

public class BlackBoxExceptionTest {

    @Test
    public void testConstructor() {
        new BlackBoxException("test");
    }
}
