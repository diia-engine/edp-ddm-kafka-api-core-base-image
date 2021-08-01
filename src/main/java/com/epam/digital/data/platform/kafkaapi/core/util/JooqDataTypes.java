package com.epam.digital.data.platform.kafkaapi.core.util;

import com.epam.digital.data.platform.kafkaapi.core.converter.jooq.FileConverter;
import com.epam.digital.data.platform.model.core.kafka.File;
import org.jooq.DataType;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

public final class JooqDataTypes {

  public static final DataType<File> FILE_DATA_TYPE =
      SQLDataType.OTHER.asConvertedDataType(new FileConverter());

  public static final DataType<Object[]> ARRAY_DATA_TYPE =
      DefaultDataType.getDefaultDataType("java.util.Collection").getArrayDataType();

  private JooqDataTypes() {}
}
