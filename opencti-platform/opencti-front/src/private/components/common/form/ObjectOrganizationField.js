import React, { useState } from 'react';
import { AccountBalanceOutlined } from '@mui/icons-material';
import { Field } from 'formik';
import { graphql } from 'react-relay';
import Alert from '@mui/material/Alert';
import AlertTitle from '@mui/material/AlertTitle';
import { makeStyles } from '@mui/styles';
import { fetchQuery } from '../../../../relay/environment';
import AutocompleteField from '../../../../components/AutocompleteField';
import { useFormatter } from '../../../../components/i18n';

const useStyles = makeStyles(() => ({
  icon: {
    paddingTop: 4,
    display: 'inline-block',
  },
  text: {
    display: 'inline-block',
    flexGrow: 1,
    marginLeft: 10,
  },
  message: {
    width: '100%',
    overflow: 'hidden',
  },
}));

export const searchObjectOrganizationFieldQuery = graphql`
  query ObjectOrganizationFieldQuery {
    organizations {
      edges {
        node {
          id
          name
        }
      }
    }
  }
`;

const ObjectOrganizationField = (props) => {
  const {
    name,
    label,
    style,
    onChange,
    helpertext,
    disabled,
    defaultOrganizations,
    outlined = true,
    multiple = true,
  } = props;

  const defaultStateOrganizations = (defaultOrganizations ?? []).map((n) => ({ label: n.name, value: n.id }));
  const [organizations, setOrganizations] = useState(defaultStateOrganizations);
  const classes = useStyles();
  const { t } = useFormatter();

  const searchOrganizations = () => {
    fetchQuery(searchObjectOrganizationFieldQuery)
      .toPromise()
      .then((data) => {
        const searchResults = data.organizations.edges.map((n) => ({ label: n.node.name, value: n.node.id }));
        setOrganizations(searchResults);
      });
  };

  if (outlined === false) {
    return (
      <Field
        component={AutocompleteField}
        name={name}
        multiple={true}
        disabled={disabled}
        style={style}
        textfieldprops={{
          variant: 'standard',
          label: t(label ?? 'Organizations restriction'),
          helperText: helpertext,
          fullWidth: true,
          onFocus: searchOrganizations,
        }}
        noOptionsText={t('No available options')}
        options={organizations}
        onInputChange={searchOrganizations}
        onChange={typeof onChange === 'function' ? onChange : null}
        renderOption={(renderProps, option) => (
          <li {...renderProps}>
            <div className={classes.icon} style={{ color: option.color }}>
              <AccountBalanceOutlined />
            </div>
            <div className={classes.text}>{option.label}</div>
          </li>
        )}
      />
    );
  }
  return (
    <Alert severity="warning" variant="outlined" style={style} classes={{ message: classes.message }}>
      <AlertTitle>{t(label ?? 'Organizations restriction')}</AlertTitle>
      <Field
        component={AutocompleteField}
        name={name}
        multiple={multiple}
        disabled={disabled}
        style={{ width: '100%', marginTop: 10 }}
        textfieldprops={{
          variant: 'standard',
          helperText: helpertext,
          fullWidth: true,
          onFocus: searchOrganizations,
        }}
        noOptionsText={t('No available options')}
        options={organizations}
        onInputChange={searchOrganizations}
        onChange={typeof onChange === 'function' ? onChange : null}
        renderOption={(renderProps, option) => (
          <li {...renderProps}>
            <div className={classes.icon} style={{ color: option.color }}>
              <AccountBalanceOutlined />
            </div>
            <div className={classes.text}>{option.label}</div>
          </li>
        )}
      />
    </Alert>
  );
};

export default ObjectOrganizationField;
