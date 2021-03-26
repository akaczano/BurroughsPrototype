import React from 'react';
import { connect } from 'react-redux';
import { Modal, Table, Button, Spinner } from 'react-bootstrap';
import { BsFillTrashFill } from 'react-icons/bs';

import { closeDescription, deleteTopic } from '../actions/topicActions';

class SchemaDisplay extends React.Component {

    getError() {
        if (this.props.error) {
            return (
                <p className="delete-error">
                    {this.props.error}
                </p>
            );
        }
        return null;
    }

    render() {
        const getButtonConntent = () => {
            if (this.props.deleting) {
                return <Spinner animation="border" />;
            }
            else {
                return <BsFillTrashFill />;
            }

        }
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
                    <Button 
                        variant="info" 
                        className="deleteButton"
                        onClick={e => {
                            this.props.deleteTopic(this.props.schema?.topic);
                        }}
                    >
                        {getButtonConntent()}
                    </Button>
                </Modal.Header>
                <Modal.Body>
                    {this.getError()}
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
                                    <tr key={item.name}>
                                        <td>{item.name}</td>
                                        <td>{item.type}</td>
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
        schema: state.topic.topicSchema,
        deleting: state.topic.topicDeleting,
        error: state.topic.topicDeleteError
    };
};

export default connect(mapStateToProps, { closeDescription, deleteTopic })(SchemaDisplay);