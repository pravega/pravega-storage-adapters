/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage;

import java.io.IOException;

/**
 * Exception that indicates a malformed file name.
 */
public class BindingException extends IOException {

    private static final long serialVersionUID = 1L;

    BindingException(String bindingName, String message) {
        super(getMessage(bindingName, message));
    }

    BindingException(String bindingName, String message, Throwable cause) {
        super(getMessage(bindingName, message), cause);
    }

    private static String getMessage(String bindingName, String message) {
        return String.format("Invalid binding  '%s'. %s", bindingName, message);
    }
}
