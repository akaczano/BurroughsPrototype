import React from 'react';
import { connect } from 'react-redux';
import { Row, Button } from 'react-bootstrap';

import { getConnection, reconnect, loadDatabase } from '../actions/basicActions';

class Header extends React.Component {

    componentDidMount() {
        this.props.getConnection();
        setInterval(this.props.getConnection, 5000);
    }


    getConnectionLabel(c) {
        if (!this.props.connection) {
            return <span style={{ color: 'lightgray' }}>Getting status...</span>
        }
        else {
            let connected = (this.props.connection.ksqlConnected && c) ||
                (this.props.connection.dbConnected && !c);
            if (connected) {                
                return <span style={{ color: '#1b872f' }}>Connected</span>
            }
            else {
                return <span style={{ color: 'red' }}>Disconnected</span>
            }
        }
    }

    render() {
        return (
            <Row style={{ padding: '7px', margin: '0px', backgroundColor: '#e6e6e6', color: 'dark-gray', borderBottom: '2px ridge #7d1127'}}>
                <Row style={{ position: 'absolute', right: '0px', marginRight: '20px' }}>
                    <span style={{fontSize: '12px'}}>
                        KsqlDB: {this.getConnectionLabel(true)}
                        <br />
                        Database: {this.getConnectionLabel(false)}
                    </span>
                    <Button
                        variant="dark" 
                        style={{ marginLeft: '8px', height: '35px', fontSize: '15px'}}
                        onClick={() => this.props.reconnect()}
                    >
                        Reconnect
                    </Button>
                </Row>
                <h3 style={{ float: 'left', fontFamil: 'Tahoma' }}>Burroughs</h3>
            </Row>
        );
    }
}

const mapStateToProps = state => {
    return {
        connection: state.core.connection
    };
};

export default connect(mapStateToProps, { getConnection, reconnect, loadDatabase })(Header);