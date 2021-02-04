import React from 'react';
import { connect } from 'react-redux';
import { Modal, Table } from 'react-bootstrap';

import { closeDescription } from '../actions/basicActions';

class SchemaDisplay extends React.Component {
    render() {
        return (
            <Modal
                size="lg"
                show={this.props.schema !== null}
                onHide={() => this.props.closeDescription()}
                aria-labelledby="example-modal-sizes-title-lg"
            >
                <Modal.Header closeButton>
                    <Modal.Title >
                        {this.props.schema?.topic}
                    </Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Table striped bordered hover>
                        <thead>
                            <tr>
                                <th>Field Name</th>
                                <th>Type</th>
                            </tr>                            
                        </thead>
                        <tbody>
                            {this.props.schema?.data.map(item => {
                                return (
                                    <tr>
                                        <td>{item.name}</td>
                                        <td>{item.schema.type}</td>
                                    </tr>
                                );
                            })}
                            </tbody>
                    </Table>
                </Modal.Body>
            </Modal>
        );
    }
}

const mapStateToProps = state => {
    return {
        schema: state.core.topicSchema
    };
};

export default connect(mapStateToProps, { closeDescription })(SchemaDisplay);