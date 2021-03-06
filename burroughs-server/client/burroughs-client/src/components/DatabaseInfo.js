import React from 'react';
import { Form } from 'react-bootstrap';
import { connect } from 'react-redux';

import { setOutputTable, loadDatabase } from '../actions/basicActions';

class DatabaseInfo extends React.Component {

    componentDidMount() {
        this.props.loadDatabase();
    }

    render() {
        return (
            <Form style={{ margin: '1px', marginLeft: '18px', width: '100%'}} className="db-info">
                <Form.Row style={{backgroundColor: '#e6e6e6', width: '100%', padding: '5px', borderBottom: '1.5px ridge gray'}}>
                <strong style={{marginLeft: '5px', color: 'black' }}>
                    Database Info</strong>
                </Form.Row>
                <Form.Group>
                    <Form.Label>
                        Output Table
                    </Form.Label>
                    <Form.Control 
                        type="text" 
                        style={{ marginRight: '10px' }} 
                        value={this.props.table}
                        onChange={e => this.props.setOutputTable(e.target.value)}
                    />
                </Form.Group>
                <Form.Group>
                    <Form.Label>
                        Hostname
                    </Form.Label>
                    <Form.Control type="text" readOnly={true} value={this.props.hostname} />
                </Form.Group>
                <Form.Group>
                    <Form.Label>
                        User
                    </Form.Label>
                    <Form.Control type="text" readOnly={true} value={this.props.user} />
                </Form.Group>                
                <Form.Group>
                    <Form.Label>
                        Database
                    </Form.Label>
                    <Form.Control type="text" readOnly={true} value={this.props.database}/>
                </Form.Group>
            </Form>
        );
    }
}

const mapStateToProps = state => {
    return {
        table: state.core.dbInfo.table,
        database: state.core.dbInfo.database,
        user: state.core.dbInfo.username,
        hostname: state.core.dbInfo.hostname
    };
};

export default connect(mapStateToProps, { setOutputTable, loadDatabase })(DatabaseInfo);