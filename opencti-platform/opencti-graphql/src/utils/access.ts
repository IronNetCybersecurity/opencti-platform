import * as R from 'ramda';
import type { Request } from 'express';
import type { Context, Span, Tracer } from '@opentelemetry/api';
import { context as telemetryContext, trace } from '@opentelemetry/api';
import { OPENCTI_SYSTEM_UUID } from '../schema/general';
import { basePath, baseUrl } from '../config/conf';
import type { AuthContext, AuthUser } from '../types/user';
import type { BasicStoreCommon, BasicStoreSettings } from '../types/store';
import { RELATION_GRANTED_TO, RELATION_OBJECT_MARKING } from '../schema/stixMetaRelationship';
import { getEntityFromCache } from '../database/cache';
import { ENTITY_TYPE_SETTINGS } from '../schema/internalObject';
import { STIX_EXT_OCTI } from '../types/stix-extensions';
import type { StixCoreObject } from '../types/stix-common';

export const BYPASS = 'BYPASS';
export const BYPASS_REFERENCE = 'BYPASSREFERENCE';
export const KNOWLEDGE_ORGANIZATION_RESTRICT = 'KNOWLEDGE_KNUPDATE_KNORGARESTRICT';
export const ROLE_ADMINISTRATOR = 'Administrator';
const RETENTION_MANAGER_USER_UUID = '82ed2c6c-eb27-498e-b904-4f2abc04e05f';

class TracingContext {
  ctx: Context | undefined;

  tracer: Tracer;

  constructor(tracer: Tracer) {
    this.tracer = tracer;
    this.ctx = undefined;
  }

  getCtx() {
    return this.ctx;
  }

  getTracer() {
    return this.tracer;
  }

  setCurrentCtx(span: Span) {
    this.ctx = trace.setSpan(telemetryContext.active(), span);
  }
}

export const executionContext = (source: string): AuthContext => {
  const tracer = trace.getTracer('instrumentation-opencti', '1.0.0');
  const tracing = new TracingContext(tracer);
  return { source, tracing };
};

export const SYSTEM_USER: AuthUser = {
  id: OPENCTI_SYSTEM_UUID,
  internal_id: OPENCTI_SYSTEM_UUID,
  name: 'SYSTEM',
  user_email: 'SYSTEM',
  inside_platform_organization: true,
  origin: { user_id: OPENCTI_SYSTEM_UUID },
  roles: [{ name: ROLE_ADMINISTRATOR }],
  capabilities: [{ name: BYPASS }],
  organizations: [],
  allowed_marking: [],
  all_marking: [],
};

export const RETENTION_MANAGER_USER: AuthUser = {
  id: RETENTION_MANAGER_USER_UUID,
  internal_id: RETENTION_MANAGER_USER_UUID,
  name: 'RETENTION MANAGER',
  user_email: 'RETENTION MANAGER',
  inside_platform_organization: true,
  origin: { user_id: RETENTION_MANAGER_USER_UUID },
  roles: [{ name: ROLE_ADMINISTRATOR }],
  capabilities: [{ name: BYPASS }],
  organizations: [],
  allowed_marking: [],
  all_marking: [],
};

export const INTERNAL_USERS = {
  [SYSTEM_USER.id]: SYSTEM_USER,
  [RETENTION_MANAGER_USER.id]: RETENTION_MANAGER_USER
};

export const getBaseUrl = (req: Request): string => {
  if (baseUrl) {
    return baseUrl;
  }
  if (req) {
    const [, port] = req.headers.host ? req.headers.host.split(':') : [];
    const isCustomPort = port !== '80' && port !== '443';
    const httpPort = isCustomPort && port ? `:${port}` : '';
    return `${req.protocol}://${req.hostname}${httpPort}${basePath}`;
  }
  return basePath;
};

export const isBypassUser = (user: AuthUser): boolean => {
  return R.find((s) => s.name === BYPASS, user.capabilities || []) !== undefined;
};

export const filterStoreElements = async (context: AuthContext, user: AuthUser, elements: Array<BasicStoreCommon>) => {
  // If user have bypass, grant access to all
  if (isBypassUser(user)) {
    return elements;
  }
  // If not filter by the inner markings
  const settings = await getEntityFromCache(context, user, ENTITY_TYPE_SETTINGS);
  const authorizedMarkings = user.allowed_marking.map((a) => a.internal_id);
  return elements.filter((element) => {
    // Markings
    const elementMarkings = element[RELATION_OBJECT_MARKING] ?? [];
    if (elementMarkings.length > 0) {
      const markingAllowed = elementMarkings.every((m) => authorizedMarkings.includes(m));
      if (!markingAllowed) {
        return false;
      }
    }
    // Organizations
    const elementOrganizations = element[RELATION_GRANTED_TO] ?? [];
    const userOrganizations = user.organizations.map((o) => o.internal_id);
    if (settings.platform_organization) {
      if (user.inside_platform_organization) {
        return true;
      }
      return elementOrganizations.some((r) => userOrganizations.includes(r));
    }
    return elementOrganizations.length === 0 || elementOrganizations.some((r) => userOrganizations.includes(r));
  });
};

export const isUserCanAccessStoreElement = async (context: AuthContext, user: AuthUser, element: BasicStoreCommon) => {
  const elements = await filterStoreElements(context, user, [element]);
  return elements.length === 1;
};

export const isUserCanAccessStixElement = async (context: AuthContext, user: AuthUser, instance: StixCoreObject) => {
  // If user have bypass, grant access to all
  if (isBypassUser(user)) {
    return true;
  }
  // Markings
  const instanceMarkings = instance.object_marking_refs ?? [];
  if (instanceMarkings.length > 0) {
    const userMarkings = (user.allowed_marking || []).map((m) => m.standard_id);
    const isUserHaveAccess = instanceMarkings.every((m) => userMarkings.includes(m));
    if (!isUserHaveAccess) {
      return false;
    }
  }
  /// Organizations
  const settings = await getEntityFromCache<BasicStoreSettings>(context, user, ENTITY_TYPE_SETTINGS);
  const elementOrganizations = instance.extensions[STIX_EXT_OCTI].granted_refs ?? [];
  const userOrganizations = user.organizations.map((o) => o.standard_id);
  if (settings.platform_organization) {
    if (user.inside_platform_organization) {
      return true;
    }
    return elementOrganizations.some((r) => userOrganizations.includes(r));
  }
  return elementOrganizations.length === 0 || elementOrganizations.some((r) => userOrganizations.includes(r));
};
