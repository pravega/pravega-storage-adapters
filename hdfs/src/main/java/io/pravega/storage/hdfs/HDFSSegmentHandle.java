/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.SegmentHandle;
import lombok.Getter;
import org.apache.http.annotation.ThreadSafe;

/**
 * Base Handle for HDFSStorage.
 */
@ThreadSafe
class HDFSSegmentHandle implements SegmentHandle {
    //region Members

    @Getter
    private final String segmentName;
    @Getter
    private final boolean readOnly;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *  @param segmentName The name of the Segment in this Handle, as perceived by users of the Storage interface.
     * @param readOnly    Whether this handle is read-only or not.
     */
    private HDFSSegmentHandle(String segmentName, boolean readOnly) {
        this.segmentName = Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        this.readOnly = readOnly;
    }

    /**
     * Creates a read-write handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @return The new handle.
     */
    static HDFSSegmentHandle write(String segmentName) {
        return new HDFSSegmentHandle(segmentName, false);
    }

    /**
     * Creates a read-only handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @return The new handle.
     */
    static HDFSSegmentHandle read(String segmentName) {
        return new HDFSSegmentHandle(segmentName, true);
    }

    //endregion

    //region Properties

    @Override
    public String toString() {
        return String.format("[%s] %s", this.readOnly ? "R" : "RW", this.segmentName);
    }

    //endregion
}
