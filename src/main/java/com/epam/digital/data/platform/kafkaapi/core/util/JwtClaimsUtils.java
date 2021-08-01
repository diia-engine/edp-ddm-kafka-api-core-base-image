package com.epam.digital.data.platform.kafkaapi.core.util;

import com.epam.digital.data.platform.starter.security.dto.JwtClaimsDto;

import java.util.List;

public final class JwtClaimsUtils {

  private JwtClaimsUtils() {}

  public static List<String> getRoles(JwtClaimsDto userClaims) {
    return userClaims.getRealmAccess().getRoles();
  }
}
