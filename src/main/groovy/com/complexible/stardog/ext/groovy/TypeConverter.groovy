/*
 * Copyright (c) the original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.complexible.stardog.ext.groovy

import org.openrdf.model.Value
import org.openrdf.model.impl.CalendarLiteralImpl
import org.openrdf.model.impl.LiteralImpl
import org.openrdf.model.impl.NumericLiteralImpl
import org.openrdf.model.impl.URIImpl

import javax.xml.datatype.DatatypeConfigurationException
import javax.xml.datatype.DatatypeFactory
import javax.xml.datatype.XMLGregorianCalendar

/**
 * To be replaced when commons makes its way to Maven central
 */
public class TypeConverter {

	public static Value asLiteral(Object o) {
		if (o instanceof String) {
			return asLiteral((String)o);
		} else if (o instanceof URI) {
			return asLiteral((URI) o);
		} else if (o instanceof Date) {
			return asLiteral((Date)o);
		} else if (o instanceof Integer) {
			return asLiteral((Integer) o);
		} else {
			return null;
		}
	}

	public static Value asLiteral(Date date) {
		GregorianCalendar c = new GregorianCalendar();
		c.setTime((Date)date);
		XMLGregorianCalendar date2 = null;
		try {
			date2 = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
		} catch (DatatypeConfigurationException e) {
			throw new RuntimeException(e);
		}
		CalendarLiteralImpl objectValue = new CalendarLiteralImpl(date2);
		return objectValue;
	}

	public static Value asResource(java.net.URI uri) {
		return new URIImpl(((java.net.URI)uri).toString());
	}

	public static Value asLiteral(java.net.URI uri) {
		return new LiteralImpl(((java.net.URI)uri).toString());
	}

	public static Value asLiteral(Integer i) {
		return new NumericLiteralImpl(i);
	}

	public static Value asLiteral(String s) {
		return new LiteralImpl(s);
	}

}