import React from 'react';
import { connect } from 'react-redux';
import { Form, Table, Spinner, Button } from 'react-bootstrap';
import { BsFillPlayFill } from 'react-icons/bs';

import { loadSnapshot, setQueryText } from '../actions/dataActions';

class DataView extends React.Component {
    state = { queryText: '' }
    getBody() {
        if (this.props.dataLoading) {
            return (
                <Spinner animation="border" />
            );
        }
        else if (this.props.dataError) {
            return (
                <p style={{ color: 'red' }}>{this.props.dataError}</p>
            );
        }
        else if (!this.props.data || this.props.data.length < 1) {
            return <div>No data to display</div>;
        }
        else {
            const headers = this.props.data[0];
            return (
                <Table striped bordered hover className="data-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            {headers.map(h => (
                                <th key={'header:' + h}>{h}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {this.props.data.slice(1).map((row, i) => {
                            return (
                                <tr key={row.toString()}>
                                    <td>{i + 1}</td>
                                    {row.map(item => {
                                        return (
                                            <td key={row.toString() + item}>{item}</td>
                                        );
                                    })}
                                </tr>
                            );
                        })}
                    </tbody>
                </Table>
            );
        }
    }

    render() {
        return (
            <div>
                <Form
                    style={{
                        position: 'sticky',
                        top: 0,
                        padding: '5px',
                        paddingBottom: '0px',
                        backgroundColor: 'white',
                        marginBottom: '5px',
                        borderBottom: '1px solid black'
                    }}
                    onSubmit={e => {
                        e.preventDefault();
                        this.props.loadSnapshot(this.props.queryText);
                    }}
                >
                    <Form.Row style={{
                        marginBottom: '5px',
                        marginLeft: '2px',
                    }}>
                        <Form.Control
                            className="sql-in"
                            type="text"
                            value={this.props.queryText}
                            onChange={e => this.props.setQueryText(e.target.value)}
                            style={{ width: '60vw' }}
                        />
                        <Button
                            onClick={() => this.props.loadSnapshot(this.props.queryText)}
                            variant="secondary"
                            style={{ marginLeft: '18px' }}
                            disabled={this.props.dataLoading}
                        >
                            <BsFillPlayFill
                                style={{ color: '#35e871', fontSize: '24px' }}
                            />
                        </Button>
                    </Form.Row>
                </Form>
                {this.getBody()}
            </div>
        );
    }
}

const mapStateToProps = state => {
    return {
        data: state.data.data,
        dataLoading: state.data.dataLoading,
        dataError: state.data.dataError,
        queryText: state.data.queryText
    };
};

export default connect(mapStateToProps, { loadSnapshot, setQueryText })(DataView);
