package com.epam.digital.data.platform.kafkaapi.core.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InputValidationServiceTest {

  private static final String KEY = "key";
  private static final Request<MockEntity> REQUEST = new Request<>();

  @Mock
  private DigitalSignatureService digitalSignatureService;
  @Mock
  private JwtValidationService jwtValidationService;

  private InputValidationService inputValidationService;

  @BeforeEach
  void beforeEach() {
    inputValidationService =
        new InputValidationService(digitalSignatureService, jwtValidationService);
  }

  @Test
  void expectValidationWithJwtInvalid() {
    when(jwtValidationService.isValid(REQUEST)).thenReturn(false);

    var actualResult = inputValidationService.validate("", REQUEST);

    assertThat(actualResult.isValid()).isFalse();
    assertThat(actualResult.getStatus()).isEqualTo(Status.JWT_INVALID);
  }

  @Test
  void expectValidationWithSignatureInvalid() {
    when(jwtValidationService.isValid(REQUEST)).thenReturn(true);
    when(digitalSignatureService.isSealValid(KEY, REQUEST)).thenReturn(false);

    var actualResult = inputValidationService.validate(KEY, REQUEST);

    assertThat(actualResult.isValid()).isFalse();
    assertThat(actualResult.getStatus()).isEqualTo(Status.INVALID_SIGNATURE);
  }

  @Test
  void expectValidationWithoutErrors() {
    when(jwtValidationService.isValid(REQUEST)).thenReturn(true);
    when(digitalSignatureService.isSealValid(KEY, REQUEST)).thenReturn(true);

    var actualResult = inputValidationService.validate(KEY, REQUEST);

    assertThat(actualResult.isValid()).isTrue();
    assertThat(actualResult.getStatus()).isNull();
  }
}
