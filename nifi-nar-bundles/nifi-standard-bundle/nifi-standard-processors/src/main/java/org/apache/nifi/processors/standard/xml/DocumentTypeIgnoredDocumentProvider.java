/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard.xml;

import org.apache.nifi.xml.processing.ProcessingFeature;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Document Provider implementation that allows local Document Type Declarations
 */
public class DocumentTypeIgnoredDocumentProvider extends StandardDocumentProvider {
    /**
     * Enable Document Type Declaration through disabling disallow configuration
     */
    @Override
    protected void setFeatures(DocumentBuilderFactory documentBuilderFactory) throws ParserConfigurationException {
        documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, ProcessingFeature.SECURE_PROCESSING.isEnabled());
        documentBuilderFactory.setFeature(ProcessingFeature.DISALLOW_DOCTYPE_DECL.getFeature(), false);
        documentBuilderFactory.setFeature(ProcessingFeature.LOAD_EXTERNAL_DOCTYPE.getFeature(), false);
    }
}
