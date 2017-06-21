/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.priam.aws;
/**
 * Created by aagrawal on 4/28/17.
 */
public interface S3FileSystemDecompressedMBean extends S3FileSystemMBean{
    String MBEAN_NAME = "com.priam.aws.S3FileSystemMBean:name=S3FileSystemDecompressedMBean";

}